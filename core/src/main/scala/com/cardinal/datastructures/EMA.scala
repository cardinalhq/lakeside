/*
 * Copyright (C) 2025 CardinalHQ, Inc
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

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