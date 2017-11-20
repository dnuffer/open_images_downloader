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
  val openImagesV1DatasetMetadata = DatasetMetadata(
    imagesFile = MetadataFile(
      description = "Image URLs and metadata",
      uri = "https://storage.googleapis.com/openimages/2016_08/images_2016_08_v5.tar.gz",
      tarGzFile = MetadataTarGzFile(
        name = "images_2016_08_v5.tar.gz",
        size = 1038144161L,
        md5sum = "0e47566d03759f0b69cf4a3fba29592c"
      ),
      tarFile = MetadataTarFile(
        name = "images_2016_08_v5.tar",
        size = 3362037760L,
        md5sum = "a95622f4cd44b2f52309e4cb1cea5452"
      )
    ),
    otherFiles = List(
      MetadataFile(
        description = "Machine image-level annotations (train and validation sets)",
        uri = "https://storage.googleapis.com/openimages/2016_08/machine_ann_2016_08_v3.tar.gz",
        tarGzFile = MetadataTarGzFile(
          name = "machine_ann_2016_08_v3.tar.gz",
          size = 470893902L,
          md5sum = "48a2b164a2998f4da5a78be5655bc1fb"
        ),
        tarFile = MetadataTarFile(
          name = "machine_ann_2016_08_v3.tar",
          size = 3137187840L,
          md5sum = "2c5adb88c6eee9e7c387e38f458bfb48"
        )
      ),
      MetadataFile(
        description = "Human image-level annotations (validation set)",
        uri = "https://storage.googleapis.com/openimages/2016_08/human_ann_2016_08_v3.tar.gz",
        tarGzFile = MetadataTarGzFile(
          name = "human_ann_2016_08_v3.tar.gz",
          size = 9718729L,
          md5sum = "ac549b4cc3179309a7b41b81e96c07dc"
        ),
        tarFile = MetadataTarFile(
          name = "human_ann_2016_08_v3.tar",
          size = 63856640L,
          md5sum = "f34ba48526492c52e9ea2a7577f7a7fb"
        )
      ),
    ),
    topDir = "images_2016_08",
  )

  val openImagesV2DatasetMetadata = DatasetMetadata(
    imagesFile = MetadataFile(
      description = "Image URLs and metadata",
      uri = "https://storage.googleapis.com/openimages/2017_07/images_2017_07.tar.gz",
      tarGzFile = MetadataTarGzFile(
        name = "images_2017_07.tar.gz",
        size = 1038132176L,
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
        uri = "https://storage.googleapis.com/openimages/2017_07/annotations_human_bbox_2017_07.tar.gz",
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
        uri = "https://storage.googleapis.com/openimages/2017_07/annotations_human_2017_07.tar.gz",
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
        uri = "https://storage.googleapis.com/openimages/2017_07/annotations_machine_2017_07.tar.gz",
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
        uri = "https://storage.googleapis.com/openimages/2017_07/classes_2017_07.tar.gz",
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
    ),
    topDir = "2017_07",
  )

  val openImagesV3DatasetMetadata = DatasetMetadata(
    imagesFile = MetadataFile(
      description = "Image URLs and metadata",
      uri = "https://storage.googleapis.com/openimages/2017_11/images_2017_11.tar.gz",
      tarGzFile = MetadataTarGzFile(
        name = "images_2017_11.tar.gz",
        size = 1038133015L,
        md5sum = "d840296342470becab9db7228bda86cc"
      ),
      tarFile = MetadataTarFile(
        name = "images_2017_11.tar",
        size = 3362037760L,
        md5sum = "8157f623bfe27027601af228c3ba999d"
      )
    ),
    otherFiles = List(
      MetadataFile(
        description = "Bounding box annotations (train, validation, and test sets)",
        uri = "https://storage.googleapis.com/openimages/2017_11/annotations_human_bbox_2017_11.tar.gz",
        tarGzFile = MetadataTarGzFile(
          name = "annotations_human_bbox_2017_11.tar.gz",
          size = 102011131L,
          md5sum = "427faa2f208ab358bed6a7f380a99493"
        ),
        tarFile = MetadataTarFile(
          name = "annotations_human_bbox_2017_11.tar",
          size = 341913600L,
          md5sum = "532c71692b54c44ee9860fe400cbac8b"
        )
      ),
      MetadataFile(
        description = "Human-verified image-level annotations (train, validation, and test sets)",
        uri = "https://storage.googleapis.com/openimages/2017_11/annotations_human_2017_11.tar.gz",
        tarGzFile = MetadataTarGzFile(
          name = "annotations_human_2017_11.tar.gz",
          size = 144102834L,
          md5sum = "cdcbb5e9aba900530c85cb370564f599"
        ),
        tarFile = MetadataTarFile(
          name = "annotations_human_2017_11.tar",
          size = 973137920L,
          md5sum = "7f38f44c9e4e33688dbb1ab3d014223f"
        )
      ),
      MetadataFile(
        description = "Machine-generated image-level annotations (train, validation, and test sets)",
        uri = "https://storage.googleapis.com/openimages/2017_11/annotations_machine_2017_11.tar.gz",
        tarGzFile = MetadataTarGzFile(
          name = "annotations_machine_2017_11.tar.gz",
          size = 468938323L,
          md5sum = "493e0e17f8bbb1dd1bf778b24aa11a84"
        ),
        tarFile = MetadataTarFile(
          name = "annotations_machine_2017_11.tar",
          size = 3127920640L,
          md5sum = "11fb4ac3d600548f58f660a67202ac6f"
        )
      ),
      MetadataFile(
        description = "Classes and class descriptions",
        uri = "https://storage.googleapis.com/openimages/2017_11/classes_2017_11.tar.gz",
        tarGzFile = MetadataTarGzFile(
          name = "classes_2017_11.tar.gz",
          size = 303055L,
          md5sum = "3937f6dddf777ca912c816d5f6ffe06f"
        ),
        tarFile = MetadataTarFile(
          name = "classes_2017_11.tar",
          size = 747520,
          md5sum = "b46363532cbc22a975933f4af4aede43"
        )
      ),
    ),
    topDir = "2017_11",
  )
}

case class DatasetMetadata(imagesFile: MetadataFile, otherFiles: Iterable[MetadataFile], topDir: String)