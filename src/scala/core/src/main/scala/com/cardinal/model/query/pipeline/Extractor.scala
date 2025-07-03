package com.cardinal.model.query.pipeline

import com.cardinal.utils.Commons.MESSAGE

case class ExtractedField(name: String, `type`: String)
case class Extractor(regex: String, fields: List[ExtractedField], inputField: String = MESSAGE)
