# Open Images dataset downloader

TODO:
* record failures into a .csv
* record successful downloads into a .csv
* do train, validation & test from multiple dirs
* optionally save original file and/or extract:
    * Image URLs and metadata
    * Bounding box annotations (train, validation, and test sets)
    * Image-level annotations (train, validation, and test sets)
    * Machine-populated image-level annotations (train, validation, and test sets)
    * Classes and class descriptions
* output progress to console
* save logs to a file
* optionally save metadata to a db (jpa?)
* optionally save annotations to a db (jpa?)
* optionally download 300k urls. Thumbnail300KURL field. Missing for a number of rows, use the original url in that case.
* optionally validate the image is a proper jpeg
* optionally convert from non-standard cmyk format
* optionally convert from png
* optionally discard png
* option to save or discard original image
* option to resize
    * output format
    * min/max side
    * crop to fixed aspect ratio
    * output quality/compression level
* distribute across machines using akka
* put images into filename-prefix-subdirs to avoid 9Mil files in 1 dir. Splitting on the first 3 chars might do the job. With 1 level of dirs, it's possible to create a balance with sqrt(9000000) = 3000 entries in each. Could use either the ImageId from the images.csv file (hex digits) or the beginning of the flickr filname (decimal digits).
* output to tfrecord
* display images as they are downloaded
* retry 500 server errors.
* command line options to control connection pooling and parallelism.
* save the images.csv files locally while simultaneously feeding them into the url downloading. Probably by creating a custom graph stage similar to Ops.Buffer but storing the data in a file instead of a memory buffer.
* resume downloading partially downloaded files? Seems a bit pointless, none of the files are that big. Would be interesting to implement anyway!