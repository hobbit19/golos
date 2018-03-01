#pragma once
#include <golos/protocol/operations.hpp>

#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/stream/array.hpp>
#include <bsoncxx/builder/stream/value_context.hpp>
#include <bsoncxx/json.hpp>

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/uri.hpp>


namespace golos {
namespace plugins {
namespace mongo_db {

    void write_operation(const vote_operation& op);
    void write_operation(const comment_operation& op);
    void write_operation(const transfer_operation& op);
    void write_operation(const transfer_to_vesting_operation& op);
    void write_operation(const withdraw_vesting_operation& op);
    void write_operation(const limit_order_create_operation& op);
    void write_operation(const limit_order_cancel_operation& op);
    void write_operation(const feed_publish_operation& op);
    void write_operation(const convert_operation& op);
    void write_operation(const account_create_operation& op);
    void write_operation(const account_update_operation& op);
    void write_operation(const witness_update_operation& op);
    void write_operation(const account_witness_vote_operation& op);
    void write_operation(const account_witness_proxy_operation& op);
    void write_operation(const pow_operation& op);
    void write_operation(const custom_operation& op);
    void write_operation(const report_over_production_operation& op);
    void write_operation(const delete_comment_operation& op);
    void write_operation(const custom_json_operation& op);
    void write_operation(const comment_options_operation& op);
    void write_operation(const set_withdraw_vesting_route_operation& op);
    void write_operation(const limit_order_create2_operation& op);
    void write_operation(const challenge_authority_operation& op);
    void write_operation(const prove_authority_operation& op);
    void write_operation(const request_account_recovery_operation& op);
    void write_operation(const recover_account_operation& op);
    void write_operation(const change_recovery_account_operation& op);
    void write_operation(const escrow_transfer_operation& op);
    void write_operation(const escrow_dispute_operation& op);
    void write_operation(const escrow_release_operation& op);
    void write_operation(const pow2_operation& op);
    void write_operation(const escrow_approve_operation& op);
    void write_operation(const transfer_to_savings_operation& op);
    void write_operation(const transfer_from_savings_operation& op);
    void write_operation(const cancel_transfer_from_savings_operation& op);
    void write_operation(const custom_binary_operation& op);
    void write_operation(const decline_voting_rights_operation& op);
    void write_operation(const reset_account_operation& op);
    void write_operation(const set_reset_account_operation& op);
    void write_operation(const fill_convert_request_operation& op);
    void write_operation(const author_reward_operation& op);
    void write_operation(const curation_reward_operation& op);
    void write_operation(const comment_reward_operation& op);
    void write_operation(const liquidity_reward_operation& op);
    void write_operation(const interest_operation& op);
    void write_operation(const fill_vesting_withdraw_operation& op);
    void write_operation(const fill_order_operation& op);
    void write_operation(const shutdown_witness_operation& op);
    void write_operation(const fill_transfer_from_savings_operation& op);
    void write_operation(const hardfork_operation& op);
    void write_operation(const comment_payout_update_operation& op);

}}}
