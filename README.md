# Open Images dataset downloader

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
* optionally convert from png
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
* optionally save metadata to a db (jpa?)
* optionally save annotations to a db (jpa?)
* distribute across machines using akka
* resume downloading partially downloaded files? Seems a bit pointless, none of the files are that big. Would be interesting to implement anyway! The file images_2017_07.tar.gz is the largest.
