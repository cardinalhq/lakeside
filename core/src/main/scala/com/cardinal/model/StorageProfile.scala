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

package com.cardinal.model

case class StorageProfile(
  collectorUuid: Option[String],
  collectorId: Option[String],
  instanceNum: Option[Int],
  organizationId: String,
  storageProfileId: String,
  bucket: String,
  cloudProvider: String,
  region: String,
  hosted: Boolean,
  spProperties: Map[String, Object],
  role: String,
  collectorType: Int,
  endpoint : Option[String],
  useSsl: Boolean = true
)
