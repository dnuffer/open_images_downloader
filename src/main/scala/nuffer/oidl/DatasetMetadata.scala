package nuffer.oidl

trait MetadataFileDetails {
  def name: String

  def size: Long

  def md5sum: String
}

case class MetadataTarGzFile(name: String, size: Long, md5sum: String) extends MetadataFileDetails

case class MetadataTarFile(name: String, size: Long, md5sum: String) extends MetadataFileDetails

case class MetadataFile(description: String, uri: String, tarGzFile: MetadataTarGzFile, tarFile: MetadataTarFile)


object DatasetMetadata {
  val openImagesV2DatasetMetadata = DatasetMetadata(
    imagesFile = MetadataFile(
      description = "Image URLs and metadata",
//      uri = "https://storage.googleapis.com/openimages/2017_07/images_2017_07.tar.gz",
      uri = "http://localhost:7585/images_2017_07.tar.gz",
      tarGzFile = MetadataTarGzFile(
        name = "images_2017_07.tar.gz",
        size = 1038132176,
        md5sum = "9c7d7d1b9c19f72c77ac0fa8e2695e00"
      ),
      tarFile = MetadataTarFile(
        name = "images_2017_07.tar",
        size = 3362037760L,
        md5sum = "bff4cdd922f018f343e53e2ffea909f2"
      )
    ),
    otherFiles = List(
      MetadataFile(
        description = "Bounding box annotations (train, validation, and test sets)",
//        uri = "https://storage.googleapis.com/openimages/2017_07/annotations_human_bbox_2017_07.tar.gz",
        uri = "http://localhost:7585/annotations_human_bbox_2017_07.tar.gz",
        tarGzFile = MetadataTarGzFile(
          name = "annotations_human_bbox_2017_07.tar.gz",
          size = 38732419,
          md5sum = "3c57bff20c56e50025e16562c4c0d709"
        ),
        tarFile = MetadataTarFile(
          name = "annotations_human_bbox_2017_07.tar",
          size = 141342720,
          md5sum = "927db28263ee14c741d2e7c08cdd15c7"
        )
      ),
      MetadataFile(
        description = "Human-verified image-level annotations (train, validation, and test sets)",
//        uri = "https://storage.googleapis.com/openimages/2017_07/annotations_human_2017_07.tar.gz",
        uri = "http://localhost:7585/annotations_human_2017_07.tar.gz",
        tarGzFile = MetadataTarGzFile(
          name = "annotations_human_2017_07.tar.gz",
          size = 69263638,
          md5sum = "27610fb9d349b728448507beec244e9f"
        ),
        tarFile = MetadataTarFile(
          name = "annotations_human_2017_07.tar",
          size = 415528960,
          md5sum = "31f0e58103335281d8413a67bfd526b9"
        )
      ),
      MetadataFile(
        description = "Machine-generated image-level annotations (train, validation, and test sets)",
//        uri = "https://storage.googleapis.com/openimages/2017_07/annotations_machine_2017_07.tar.gz",
        uri = "http://localhost:7585/annotations_machine_2017_07.tar.gz",
        tarGzFile = MetadataTarGzFile(
          name = "annotations_machine_2017_07.tar.gz",
          size = 468937924,
          md5sum = "c19b3fc5059ac13431b085c3e5df08af"
        ),
        tarFile = MetadataTarFile(
          name = "annotations_machine_2017_07.tar",
          size = 3127920640L,
          md5sum = "7825a907b569ccd3df88002936bead68"
        )
      ),
      MetadataFile(
        description = "Classes and class descriptions",
//        uri = "https://storage.googleapis.com/openimages/2017_07/classes_2017_07.tar.gz",
        uri = "http://localhost:7585/classes_2017_07.tar.gz",
        tarGzFile = MetadataTarGzFile(
          name = "classes_2017_07.tar.gz",
          size = 300021,
          md5sum = "a51205f02faf3867ae2d3e8dc968fd6d"
        ),
        tarFile = MetadataTarFile(
          name = "classes_2017_07.tar",
          size = 727040,
          md5sum = "20f621858305d3d56a1607a64ace4dcc"
        )
      ),
    )
  )

}

case class DatasetMetadata(imagesFile: MetadataFile, otherFiles: Iterable[MetadataFile])