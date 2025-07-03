package com.cardinal.model

import com.cardinal.discovery.Pod

case class SlotInfo(numSlots: Int, podBySlot: Map[Int, Pod])

