# Open Images dataset downloader

This program is built for downloading, verifying and resizing the images and metadata of the Open Images dataset (https://github.com/openimages/dataset). It is designed to run as fast as possible by taking advantage of the available hardware and bandwidth by using asynchronous I/O and parallelism. Each image's size and md5 sum are validated against the values found in the dataset metadata. The download results are stored in CSV files with the same format as the original images.csv so that subsequent use for training, etc. can be done knowing that all the images are available. Many (over 2%) of the original images are no longer available or have changed, so these and any other failed downloads are stored in a separate results file. If you use a program such as curl or wget to download the images, you will end up with a lot of "unavailable" png images, xml files and some images that don't match the originals. This is why it's important to validate the size and md5 sum when downloading.

The application is a command line application. If you're running it on a server, I'd recommend using screen or tmux so that it continues running if the ssh connection is interrupted.

The resizing functionality depends on the ImageMagick program `convert`, so if you want to do resizing, `convert` must be on the `PATH`. It's easy to install on a linux distribution using a package manager, and not too hard on most other OSes. See https://www.imagemagick.org/script/binary-releases.php.

This program is flexible and can be used for a number of use cases depending on how much storage you want to use and how you want to use the data. The original images can optionally be stored, and you can also choose whether to resize and store the resized images. Also the metadata download and extraction is optional. If the original images are found locally because you previously downloaded them, they will be used as the source for a resize. Also a resize is skipped if a file with size > 0 is found. This is so the program can be interrupted and restarted and you can resume it where it left off. Also if you have the original images stored locally you can resize all the images with different parameters without needing to re-download any images.

If you want to minimize the amount of space used, only store small images 224x224 stored at jpeg quality 50, and use less bandwidth by downloading the 300K urls, use the following command line options:

```bash
$ open_images_downloader --nodownload-metadata --download-300k --resize-mode FillCrop --resize-compression-quality 50  
```

If you want to save the images with a max side of 640 with original aspect ratio at original jpeg quality, and use less bandwidth by downloading the 300K urls, use the following command line options. Note that the 300K don't look as nice as the original images resized by ImageMagick. The 300k urls return images that are 640 pixels on the largest side, so the resize step only changes images that are larger than 640. Not all images have 300K urls, and in that case, the original url is used and these images are resized.

```bash
$ open_images_downloader --nodownload-metadata --download-300k --resize-box-size 640
```

If you want to download and save all the original images and metadata, and also resize them to 1024 max side, and save them in a subdirectory named images-resized-1024:

```bash
$ open_images_downloader --save-original-images --resize-box-size 1024 --resized-images-subdirectory images-resized-1024
```

There are also options for controlling how many concurrent http connections are made (don't worry about flickr, they can easily handle a few hundred connections from a single system and you downloading as fast as possible, and you won't be blocked for "abuse") which you may want to use to reduce the impact you have on your local network (you don't want your kids complaining that Netflix is "buffering" and looking all blocky, do you?!?)

Here is the complete command line help:

<TODO: copy the help to here>

TODO:

* UI
    * Show # of images/sec
    * Show # of bytes/sec read from internet
    * Show # of bytes/sec written to disk
    * Show last 100(?) successful downloads
    * Show last 100(?) failed downloads
    * Show downloads in progress w/percentage complete
    * Show # of resizes in progress 
    * Show overall number of downloads w/% of total completed
    * ncurses based progress UI
    * graphical progress UI
        * display images as they are downloaded
* optionally convert from non-standard cmyk format
* optionally convert from png. I'm not sure that there are any pngs in the dataset. The ones I saw may have just been the "unavailable image".
* optionally discard png
* optionally validate the image is a proper jpeg
    * I made a script to do this before: https://github.com/dnuffer/detect_corrupt_jpeg. It uses magic mime-type detection, and PIL open(). I also tested out using jpeginfo and ImageMagick identify.
    * There is a list of running my detect_corrupt_jpeg script at:
        * partial with jpeginfo and idenfity: /storage/data/pub/ml_datasets/openimages/images_2016_08/train/detect_corrupt_jpeg.out
        * on oi v1 with magic and PIL: /storage/data/pub/ml_datasets/openimages/images_2016_08/train/detect_corrupt_jpeg2.out  
    * Don't want to be too strict. If it's got some weird format, but can still be decoded into an image, that's fine.
    * Don't want to be too lenient. If it's missing part of the image, that should fail.
    * ImageMagick identify -verbose https://www.imagemagick.org/discourse-server/viewtopic.php?t=20045
    * The end-goal is to check it can be decoded by tensorflow into a valid image. That uses libjpeg-turbo under the covers, so I could use the program djpeg from libjpeg-turbo-progs package. I'm not sure how good it is at detecting corruption however. I'll need to experiment with it.
* option to specify original images directory
* option to enable/disable 3-letter image subdirs
* option to do multiple image processes - resize to multiple sizes, multiple resize modes, convert to other formats.

* output to tfrecord
* optionally save metadata to a db (jpa?, shiny?)
* optionally save annotations to a db (jpa?, shiny?)
* distribute across machines using akka
* resume downloading partially downloaded files? Seems a bit pointless, none of the files are that big. Would be interesting to implement anyway! The file images_2017_07.tar.gz is the largest.
* check the license (scrape the website?)