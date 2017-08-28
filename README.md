# Open Images dataset downloader

TODO:
* optionally save original file and/or extract:
    * Bounding box annotations (train, validation, and test sets)
    * Image-level annotations (train, validation, and test sets)
    * Machine-populated image-level annotations (train, validation, and test sets)
    * Classes and class descriptions
* output progress to console
* option to resize
    * output format
    * min/max side
    * crop to fixed aspect ratio
    * output quality/compression level
* optionally download 300k urls. Thumbnail300KURL field. Missing for a number of rows, use the original url in that case.
* option to save or discard original image
* option to save logs to a file
* Use sbt-native-packager to create release files https://github.com/sbt/sbt-native-packager

* ncurses based progress UI
* graphical progress UI
    * display images as they are downloaded
* optionally convert from non-standard cmyk format
* optionally convert from png
* optionally discard png
* optionally validate the image is a proper jpeg

* output to tfrecord
* optionally save metadata to a db (jpa?)
* optionally save annotations to a db (jpa?)
* distribute across machines using akka
* command line options to control connection pooling and parallelism.
* resume downloading partially downloaded files? Seems a bit pointless, none of the files are that big. Would be interesting to implement anyway! The file images_2017_07.tar.gz is the largest.
