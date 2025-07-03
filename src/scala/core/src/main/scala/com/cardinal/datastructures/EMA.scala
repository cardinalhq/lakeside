package com.cardinal.datastructures

class EMA(alpha: Double) {
  require(0 <= alpha && alpha <= 1, "Alpha should be between 0 and 1 inclusive.")

  private var ema: Option[Double] = None

  def update(value: Double): Double = {
    ema = Some(ema match {
      case Some(prevEma) => alpha * value + (1 - alpha) * prevEma
      case None => value // If this is the first value, initialize the EMA to that value.
    })
    ema.get
  }

  def set(value: Double): Unit = {
    ema = Some(value)
  }

  def value: Option[Double] = ema
}