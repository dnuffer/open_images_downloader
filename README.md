# Open Images dataset downloader

TODO:
* output progress messages to console
* optionally download 300k urls. Thumbnail300KURL field. Missing for a number of rows, use the original url in that case.
* create command line options to control behavior:
    * download 300k urls.
* Use sbt-native-packager to create release files https://github.com/sbt/sbt-native-packager

* UI
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

* output to tfrecord
* optionally save metadata to a db (jpa?)
* optionally save annotations to a db (jpa?)
* distribute across machines using akka
* command line options to control connection pooling and parallelism.
* resume downloading partially downloaded files? Seems a bit pointless, none of the files are that big. Would be interesting to implement anyway! The file images_2017_07.tar.gz is the largest.
