package com.cardinal.utils.ast

case class SketchGroup(timestamp: Long, group: Map[BaseExpr, List[SketchInput]])
