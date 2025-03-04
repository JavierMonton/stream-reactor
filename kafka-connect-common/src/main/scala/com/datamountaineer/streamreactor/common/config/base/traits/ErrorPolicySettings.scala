/*
 * Copyright 2017-2023 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datamountaineer.streamreactor.common.config.base.traits

import com.datamountaineer.streamreactor.common.errors
import com.datamountaineer.streamreactor.common.errors.ErrorPolicy
import com.datamountaineer.streamreactor.common.errors.ErrorPolicyEnum
import com.datamountaineer.streamreactor.common.config.base.const.TraitConfigConst.ERROR_POLICY_PROP_SUFFIX

trait ErrorPolicySettings extends BaseSettings {
  def errorPolicyConst = s"$connectorPrefix.$ERROR_POLICY_PROP_SUFFIX"

  def getErrorPolicy: ErrorPolicy =
    errors.ErrorPolicy(ErrorPolicyEnum.withName(getString(errorPolicyConst).toUpperCase))
}
