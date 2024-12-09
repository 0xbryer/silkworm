/*
   Copyright 2024 The Silkworm Authors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include "stage_triggers.hpp"

#include <gsl/util>

#include <silkworm/core/common/assert.hpp>
#include <silkworm/infra/concurrency/spawn.hpp>

namespace silkworm::stagedsync {

TriggersStage::TriggersStage(SyncContext* sync_context)
    : Stage(sync_context, db::stages::kTriggersStageKey) {
}

Stage::Result TriggersStage::forward(db::RWTxn& tx) {
    current_tx_ = &tx;
    SILK_INFO_M("TriggersStage", {"op", "Forward", "current_tx_", std::to_string(intptr_t(current_tx_))}) << "START";
    [[maybe_unused]] auto _ = gsl::finally([this] {
        SILK_INFO_M("TriggersStage", {"op", "Forward", "current_tx_", std::to_string(intptr_t(current_tx_))}) << "finally";
        current_tx_ = nullptr;
    });

    ioc_.restart();
    SILK_INFO_M("TriggersStage", {"op", "Forward", "current_tx_", std::to_string(intptr_t(current_tx_))}) << "AFTER restart";
    ioc_.run();

    SILK_INFO_M("TriggersStage", {"op", "Forward", "current_tx_", std::to_string(intptr_t(current_tx_))}) << "END";
    return Stage::Result::kSuccess;
}

Task<void> TriggersStage::schedule(std::function<void(db::RWTxn&)> callback) {
    auto task_caller = [this, c = std::move(callback)]() -> Task<void> {
        db::RWTxn* tx = this->current_tx_;
        SILK_INFO_M("TriggersStage", {"op", "schedule", "current_tx_", std::to_string(intptr_t(tx))}) << "callback";
        // SILKWORM_ASSERT(tx);  // TODO(canepat) debug why this get hit after restart
        if (tx == nullptr) co_return;
        c(*tx);
        co_return;
    };
    SILK_INFO_M("TriggersStage", {"op", "schedule", "current_tx_", std::to_string(intptr_t(current_tx_))}) << "BEFORE spawn";
    co_return co_await concurrency::spawn_task(ioc_, task_caller());
}

bool TriggersStage::stop() {
    SILK_INFO_M("TriggersStage", {"op", "stop", "current_tx_", std::to_string(intptr_t(current_tx_))}) << "START";
    ioc_.stop();
    SILK_INFO_M("TriggersStage", {"op", "stop", "current_tx_", std::to_string(intptr_t(current_tx_))}) << "END";
    return Stage::stop();
}

}  // namespace silkworm::stagedsync
