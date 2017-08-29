# Open Images dataset downloader

TODO:
* option to save logs to a file
* output progress messages to console
* optionally download 300k urls. Thumbnail300KURL field. Missing for a number of rows, use the original url in that case.
* create command line options to control behavior:
    * download & extract metadata files
    * save .tar.gz files
    * save .tar files
    * save .tar contents
    * option to resize
        * output format
        * min/max side
        * crop to fixed aspect ratio
        * output quality/compression level
    * save or discard original image
    * download 300k urls.
    * output dir(s)
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
