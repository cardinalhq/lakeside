package com.cardinal.model

case class DownloadSegmentRequest(bucketName: String, key: String) {
  def id: String = {
    s"${this.bucketName}/${this.key}"
  }
}
