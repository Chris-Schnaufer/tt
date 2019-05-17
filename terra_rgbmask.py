import os
import numpy as np
import tempfile
import shutil

from osgeo import gdal
from PIL import Image

from pyclowder.utils import CheckMessage
from pyclowder.datasets import download_metadata, upload_metadata, remove_metadata
from terrautils.metadata import get_extractor_metadata, get_terraref_metadata
from terrautils.extractors import TerrarefExtractor, is_latest_file, check_file_in_dataset, \
    build_metadata, upload_to_dataset, file_exists, contains_required_files, \
    confirm_clowder_info, timestamp_to_terraref
from terrautils.formats import create_geotiff, compress_geotiff
from terrautils.spatial import geojson_to_tuples
from terrautils.imagefile import file_is_image_type, image_get_geobounds, get_epsg

from skimage import morphology

import cv2


SATURATE_THRESHOLD = 245
MAX_PIXEL_VAL = 255
SMALL_AREA_THRESHOLD = 200

def getImageQuality(imgfile):
    img = Image.open(imgfile)
    img = np.array(img)

    NRMAC = MAC(img, img, img)

    return NRMAC

def gen_plant_mask(colorImg, kernelSize=3):
    r = colorImg[:, :, 2]
    g = colorImg[:, :, 1]
    b = colorImg[:, :, 0]

    sub_img = (g.astype('int') - r.astype('int') - 0) > 0  # normal: -2

    mask = np.zeros_like(b)

    mask[sub_img] = MAX_PIXEL_VAL

    blur = cv2.blur(mask, (kernelSize, kernelSize))
    pix = np.array(blur)
    sub_mask = pix > 128

    mask_1 = np.zeros_like(b)
    mask_1[sub_mask] = MAX_PIXEL_VAL

    return mask_1

def remove_small_area_mask(maskImg, min_area_size):
    mask_array = maskImg > 0
    rel_array = morphology.remove_small_objects(mask_array, min_area_size)

    rel_img = np.zeros_like(maskImg)
    rel_img[rel_array] = MAX_PIXEL_VAL

    return rel_img

def remove_small_holes_mask(maskImg, max_hole_size):
    mask_array = maskImg > 0
    rel_array = morphology.remove_small_holes(mask_array, max_hole_size)
    rel_img = np.zeros_like(maskImg)
    rel_img[rel_array] = MAX_PIXEL_VAL

    return rel_img

def saturated_pixel_classification(gray_img, baseMask, saturatedMask, dilateSize=0):
    # add saturated area into basic mask
    saturatedMask = morphology.binary_dilation(saturatedMask, morphology.diamond(dilateSize))

    rel_img = np.zeros_like(gray_img)
    rel_img[saturatedMask] = MAX_PIXEL_VAL

    label_img, num = morphology.label(rel_img, connectivity=2, return_num=True)

    rel_mask = baseMask

    for i in range(1, num):
        x = (label_img == i)

        if np.sum(x) > 100000:  # if the area is too large, do not add it into basic mask
            continue

        if not (x & baseMask).any():
            continue

        rel_mask = rel_mask | x

    return rel_mask

def over_saturation_pocess(rgb_img, init_mask, threshold=SATURATE_THRESHOLD):
    # connected component analysis for over saturation pixels
    gray_img = cv2.cvtColor(rgb_img, cv2.COLOR_BGR2GRAY)

    mask_over = gray_img > threshold

    mask_0 = gray_img < threshold

    src_mask_array = init_mask > 0

    mask_1 = src_mask_array & mask_0

    mask_1 = morphology.remove_small_objects(mask_1, SMALL_AREA_THRESHOLD)

    mask_over = morphology.remove_small_objects(mask_over, SMALL_AREA_THRESHOLD)

    rel_mask = saturated_pixel_classification(gray_img, mask_1, mask_over, 1)
    rel_img = np.zeros_like(gray_img)
    rel_img[rel_mask] = MAX_PIXEL_VAL

    return rel_img

def gen_saturated_mask(img, kernelSize):
    binMask = gen_plant_mask(img, kernelSize)
    binMask = remove_small_area_mask(binMask,
                                     500)  # 500 is a parameter for number of pixels to be removed as small area
    binMask = remove_small_holes_mask(binMask,
                                      300)  # 300 is a parameter for number of pixels to be filled as small holes

    binMask = over_saturation_pocess(img, binMask, SATURATE_THRESHOLD)

    binMask = remove_small_holes_mask(binMask, 4000)

    return binMask

def gen_mask(img, kernelSize):
    binMask = gen_plant_mask(img, kernelSize)
    binMask = remove_small_area_mask(binMask, SMALL_AREA_THRESHOLD)
    binMask = remove_small_holes_mask(binMask,
                                      3000)  # 3000 is a parameter for number of pixels to be filled as small holes

    return binMask

def gen_rgb_mask(img, binMask):
    rgbMask = cv2.bitwise_and(img, img, mask=binMask)

    return rgbMask

def rgb2gray(rgb):
    r, g, b = rgb[:,:,0], rgb[:,:,1], rgb[:,:,2]
    gray = 0.2989 * r + 0.5870 * g + 0.1140 * b
    return gray

def MAC(im1, im2, im):  # main function: Multiscale Autocorrelation (MAC)
    h, v, c = im1.shape
    if c > 1:
        im = np.matrix.round(rgb2gray(im))
        im1 = np.matrix.round(rgb2gray(im1))
        im2 = np.matrix.round(rgb2gray(im2))
    # multiscale parameters
    scales = np.array([2, 3, 5])
    FM = np.zeros(len(scales))
    for s in range(len(scales)):
        im1[0: h - 1, :] = im[1:h, :]
        im2[0: h - scales[s], :] = im[scales[s]:h, :]
        dif = im * (im1 - im2)
        FM[s] = np.mean(dif)
    NRMAC = np.mean(FM)
    return NRMAC

def check_saturation(img):
    # check how many percent of pix close to 255 or 0
    grayImg = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    m1 = grayImg > SATURATE_THRESHOLD
    m2 = grayImg < 20  # 20 is a threshold to classify low pixel value

    over_rate = float(np.sum(m1)) / float(grayImg.size)
    low_rate = float(np.sum(m2)) / float(grayImg.size)

    return over_rate, low_rate

def check_brightness(img):
    # gen average pixel value from grayscale image
    grayImg = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    aveValue = np.average(grayImg)

    return aveValue

def gen_cc_enhanced(input_path, kernelSize=3):
    # abandon low quality images, mask enhanced
    # TODO: cv2 has problems with some RGB geotiffs...
    # img = cv2.imread(input_path)
    img = np.rollaxis(gdal.Open(input_path).ReadAsArray().astype(np.uint8), 0, 3)
    img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)

    # calculate image scores
    over_rate, low_rate = check_saturation(img)

    # TODO: disabling this check for now because it's crashing extractor - generate mask regardless
    # if low score, return None
    # low_rate is percentage of low value pixels(lower than 20) in the grayscale image, if low_rate > 0.1, return
    # aveValue is average pixel value of grayscale image, if aveValue lower than 30 or higher than 195, return
    # quality_score is a score from Multiscale Autocorrelation (MAC), if quality_score lower than 13, return

    #aveValue = check_brightness(img)
    #quality_score = getImageQuality(input_path)
    #if low_rate > 0.1 or aveValue < 30 or aveValue > 195 or quality_score < 13:
    #    return None, None, None

    # saturated image process
    # over_rate is percentage of high value pixels(higher than SATUTATE_THRESHOLD) in the grayscale image, if over_rate > 0.15, try to fix it use gen_saturated_mask()
    if over_rate > 0.15:
        binMask = gen_saturated_mask(img, kernelSize)
    else:  # nomal image process
        binMask = gen_mask(img, kernelSize)

    c = np.count_nonzero(binMask)
    ratio = c / float(binMask.size)

    rgbMask = gen_rgb_mask(img, binMask)

    return ratio, rgbMask

def find_terraref_files(resource):
    """Returns the left, and right image file names
    Args:
        resource(dict): the resource associated with the process request
    Return:
        A tuple containing the left file name followed by the right filename
    Exception:
        A ValueError is raised if all the fields aren't found
    """
    img_left, img_right = None, None

    for fname in resource['local_paths']:
        if fname.endswith('_left.tif'):
            img_left = fname
        elif fname.endswith('_right.tif'):
            img_right = fname

    if None in [img_left, img_right]:
        raise ValueError("could not locate all files in processing")

    return (img_left, img_right)

def find_image_files(identify_binary, resource, file_metadata_ending):
    """Returns the left, and right image file names
    Args:
        identify_binary(str): path to the executable which will return a MIME type on an image file
        resource(dict): the resource associated with the process request
        file_metadata_ending(str): the filename ending identifying an associated metadata file
    Return:
        A tuple of found image files
    """
    return_files = []

    for fname in resource['local_paths']:
        if file_is_image_type(identify_binary, fname, fname + file_metadata_ending):
            return_files.append(fname)

    return tuple(return_files)

def add_local_arguments(parser):
    """ Add any additional arguments to parser for rgbEnhancementExtractor class
    Args:
        parse: the command argument parser
    """
    # For only processing the TERRA REF left image
    parser.add_argument('--left', type=bool, default=os.getenv('LEFT_ONLY', True),
                        help="only generate a mask for the left image")

    # Name of an image, or file, MIME type app
    identify_binary = os.getenv('IDENTIFY_BINARY', '/usr/bin/identify')

    # Command line override of an image or file MIME type app
    parser.add_argument('--identify-binary', nargs='?', dest='identify_binary',
                             default=identify_binary,
                             help='Identify executable used to for image type capture ' +
                             '(default=' + identify_binary + ')')

class rgbEnhancementExtractor(TerrarefExtractor):

    def __init__(self):
        super(rgbEnhancementExtractor, self).__init__()

        add_local_arguments(self.parser)

        # parse command line and load default logging configuration
        self.setup(sensor='rgb_mask')

        # assign local arguments
        self.leftonly = self.args.left

    def get_maskfilename_bounds(self, file_name, datestamp):
        """Determines the name of the masking file and loads the boundaries of the file
        Args:
            file_name(str): path of the file to create a mask from
            datestamp(str): the date to use when creating file paths
        Return:
        """
        mask_name, bounds = (None, None)

        if not self.get_terraref_metadata is None:
            key = 'left' if file_name.endswith('_left.tif') else 'right'
            mask_name = self.sensors.create_sensor_path(datestamp, opts=[key])
            bounds = geojson_to_tuples(self.get_terraref_metadata['spatial_metadata'][key]['bounding_box'])
        else:
            mask_name = self.sensors.create_sensor_path(datestamp)
            bounds = image_get_geobounds(file_name)
            bounds_len = len(bounds)
            if bounds_len <= 0 or bounds[0] == np.nan:
                bounds = None

        return (mask_name, bounds)

    def check_message(self, connector, host, secret_key, resource, parameters):
        if "rulechecked" in parameters and parameters["rulechecked"]:
            return CheckMessage.download

        self.start_check(resource)

        if not is_latest_file(resource):
            self.log_skip(resource, "not latest file")
            return CheckMessage.ignore

        # Check metadata to verify we have what we need
        md = download_metadata(connector, host, secret_key, resource['id'])
        if get_terraref_metadata(md):
            # Check for a left and right TIF file - skip if not found
            # If we're only processing the left files, don't check for the right file
            needed_files = ['_left.tif']
            if not self.leftonly:
                needed_files.append('_right.tif')
            if not contains_required_files(resource, needed_files):
                self.log_skip(resource, "missing required files")
                return CheckMessage.ignore

            if get_extractor_metadata(md, self.extractor_info['name'],
                                      self.extractor_info['version']):
                # Make sure outputs properly exist
                timestamp = resource['dataset_info']['name'].split(" - ")[1]
                left_mask_tiff = self.sensors.create_sensor_path(timestamp, opts=['left'])
                right_mask_tiff = self.sensors.create_sensor_path(timestamp, opts=['right'])
                if (self.leftonly and file_exists(left_mask_tiff)) or \
                   (not (file_exists(left_mask_tiff) and file_exists(right_mask_tiff))):
                    self.log_skip(resource, "metadata v%s and outputs already exist" % \
                                  self.extractor_info['version'])
                    return CheckMessage.ignore
        # Check for other images to create a mask on
        elif not contains_required_files(resource, ['.tif']):
            self.log_skip(resource, "missing required tiff file")
            return CheckMessage.ignore

        # Have TERRA-REF metadata, but not any from this extractor
        return CheckMessage.download

    def process_message(self, connector, host, secret_key, resource, parameters):

        super(rgbEnhancementExtractor, self).process_message(connector, host, secret_key,
                                                             resource, parameters)

        self.start_message(resource)

        # Get left/right files and metadata
        process_files = []
        if not self.get_terraref_metadata is None:
            process_files = find_terraref_files(resource)
        else:
            process_files = find_image_files(self.args.identify_binary, resource,
                                             self.file_infodata_file_ending)

        # Get the best username, password, and space
        old_un, old_pw, old_space = (self.clowder_user, self.clowder_pass, self.clowderspace)
        self.clowder_user, self.clowder_pass, self.clowderspace = self.get_clowder_context()

        # Ensure that the clowder information is valid
        if not confirm_clowder_info(host, secret_key, self.clowderspace, self.clowder_user,
                                    self.clowder_pass):
            self.log_error(resource, "Clowder configuration is invalid. Not processing " +\
                                     "request")
            self.clowder_user, self.clowder_pass, self.clowderspace = (old_un, old_pw, old_space)
            self.end_message(resource)
            return

        # Change the base path of files to include the user by tweaking the sensor's value
        sensor_old_base = None
        if self.get_terraref_metadata is None:
            _, new_base = self.get_username_with_base_path(host, secret_key, resource['id'],
                                                           self.sensors.base)
            sensor_old_base = self.sensors.base
            self.sensors.base = new_base

        # Prepare for processing files
        timestamp = timestamp_to_terraref(self.find_timestamp(resource['dataset_info']['name']))
        target_dsid = resource['id']
        uploaded_file_ids = []
        ratios = []

        try:
            for one_file in process_files:

                mask_source = one_file

                # Make sure the source image is in the correct EPSG space
                epsg = get_epsg(one_file)
                if epsg != self.default_epsg:
                    self.log_info(resource, "Reprojecting from " + str(epsg) +
                                  " to default " + str(self.default_epsg))
                    _, tmp_name = tempfile.mkstemp()
                    src = gdal.Open(one_file)
                    gdal.Warp(tmp_name, src, dstSRS='EPSG:'+str(self.default_epsg))
                    mask_source = tmp_name

                # Get the bounds of the image to see if we can process it. Also get the mask filename
                rgb_mask_tif, bounds = self.get_maskfilename_bounds(mask_source, timestamp)

                if bounds is None:
                    self.log_skip(resource, "Skipping non-georeferenced image: " + \
                                                                    os.path.basename(one_file))
                    if mask_source != one_file:
                        os.remove(mask_source)
                    continue

                if not file_exists(rgb_mask_tif) or self.overwrite:
                    self.log_info(resource, "creating %s" % rgb_mask_tif)

                    mask_ratio, mask_rgb = gen_cc_enhanced(mask_source)
                    ratios.append(mask_ratio)

                    # Bands must be reordered to avoid swapping R and B
                    mask_rgb = cv2.cvtColor(mask_rgb, cv2.COLOR_BGR2RGB)

                    create_geotiff(mask_rgb, bounds, rgb_mask_tif, None, False, self.extractor_info,
                                   self.get_terraref_metadata)
                    compress_geotiff(rgb_mask_tif)

                    # Remove any temporary file
                    if mask_source != one_file:
                        os.remove(mask_source)

                    self.created += 1
                    self.bytes += os.path.getsize(rgb_mask_tif)

                found_in_dest = check_file_in_dataset(connector, host, secret_key, target_dsid,
                                                      rgb_mask_tif, remove=self.overwrite)
                if not found_in_dest:
                    self.log_info(resource, "uploading %s" % rgb_mask_tif)
                    fileid = upload_to_dataset(connector, host, self.clowder_user, self.clowder_pass,
                                               target_dsid, rgb_mask_tif)
                    uploaded_file_ids.append(host + ("" if host.endswith("/") else "/") +
                                             "files/" + fileid)

            # Tell Clowder this is completed so subsequent file updates don't daisy-chain
            if not self.get_terraref_metadata is None:
                ratios_len = len(ratios)
                left_ratio = (ratios[0] if ratios_len > 0 else None)
                right_ratio = (ratios[1] if ratios_len > 1 else None)
                md = {
                    "files_created": uploaded_file_ids
                }
                if not left_ratio is None:
                    md["left_mask_ratio"] = left_ratio
                if not self.leftonly and not right_ratio is None:
                    md["right_mask_ratio"] = right_ratio
                extractor_md = build_metadata(host, self.extractor_info, target_dsid, md, 'dataset')
                self.log_info(resource, "uploading extractor metadata to Lv1 dataset")
                remove_metadata(connector, host, secret_key, resource['id'],
                                self.extractor_info['name'])
                upload_metadata(connector, host, secret_key, resource['id'], extractor_md)

        finally:
            # Signal end of processing message and restore changed variables. Be sure to restore
            # changed variables above with early returns
            if not sensor_old_base is None:
                self.sensors.base = sensor_old_base

            self.clowder_user, self.clowder_pass, self.clowderspace = (old_un, old_pw, old_space)
            self.end_message(resource)


if __name__ == "__main__":
    extractor = rgbEnhancementExtractor()
    extractor.start()
