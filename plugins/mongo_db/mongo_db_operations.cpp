#include <golos/plugins/mongo_db/mongo_db_operations.hpp>
#include <bsoncxx/builder/stream/array.hpp>
#include <bsoncxx/builder/stream/value_context.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/builder/basic/document.hpp>

namespace golos {
namespace plugins {
namespace mongo_db {

    using bsoncxx::builder::stream::array;
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::stream::document;

    // Helper functions
    document format_authority(const authority& auth) {
        array account_auths_arr;
        for (auto iter : auth.account_auths) {
            document temp;
            temp << "public_key_type" << (std::string)iter.first
                 << "weight_type" << std::to_string(iter.second);
            account_auths_arr << temp;
        }
        array key_auths_arr;
        for (auto iter : auth.key_auths) {
            document temp;
            temp << "public_key_type" << (std::string)iter.first
                 << "weight_type" << std::to_string(iter.second);
            key_auths_arr << temp;
        }

        document authority_doc;
        authority_doc << "owner" << auth.owner
                      << "weight_threshold" << std::to_string(auth.weight_threshold);
        authority_doc << "account_auths" << account_auths_arr;
        authority_doc << "key_auths" << key_auths_arr;

        return authority_doc;
    }

    document format_chain_properties(const chain_properties& props) {
        document props_doc;
        props_doc << "account_creation_fee" << props.account_creation_fee.to_string()
                  << "maximum_block_size" << std::to_string(props.maximum_block_size)
                  << "sbd_interest_rate" << std::to_string(props.sbd_interest_rate);
        return props_doc;
    }

    std::string to_string(const signature_type& signature) {
        std::string retVal;

        for (auto iter : signature) {
            retVal += iter;
        }
        return retVal;
    }
    /////////////////////////////////////////////////


    document write_operation(const operation& op) {
        return document{};
    }

    document write_operation(const vote_operation& op) {
        document retval;
        document body;

        body << "voter"  << op.voter
             << "author" << op.author
             << "permlink" << op.permlink
             << "weight" << std::to_string(op.weight);

        retval << "vote" << body;

        return retval;
    }

    document write_operation(const comment_operation& op) {
        document retval;
        document body;

        body << "parent_author"  << op.parent_author
             << "parent_permlink" << op.parent_permlink
             << "author" << op.author
             << "permlink" << op.permlink
             << "title" << op.title
             << "body" << op.body
             << "json_metadata" << op.json_metadata;

        retval << "comment" << body;

        return retval;
    }

    document write_operation(const transfer_operation& op) {
        document retval;
        document body;

        body << "from"  << op.from
             << "to" << op.to
             << "amount" << op.amount.to_string()
             << "memo" << op.memo;

        retval << "transfer" << body;

        return retval;
    }
    document write_operation(const transfer_to_vesting_operation& op) {
        document retval;
        document body;

        body << "from"  << op.from
             << "to" << op.to
             << "amount" << op.amount.to_string();

        retval << "transfer_to_vesting" << body;

        return retval;
    }
    document write_operation(const withdraw_vesting_operation& op) {
        document retval;
        document body;

        body << "account"  << op.account
             << "vesting_shares" << op.vesting_shares.to_string();

        retval << "withdraw_vesting" << body;

        return retval;
    }

    document write_operation(const limit_order_create_operation& op) {
        document retval;
        document body;

        body << "owner"  << op.owner
             << "orderid" << std::to_string(op.orderid)
             << "amount_to_sell" << op.amount_to_sell.to_string()
             << "min_to_receive" << op.min_to_receive.to_string()
             << "expiration" << op.expiration;

        retval << "limit_order_create" << body;

        return retval;
    }

    document write_operation(const limit_order_cancel_operation& op)  {
        document retval;
        document body;

        body << "owner"  << op.owner
             << "orderid" << std::to_string(op.orderid);

        retval << "limit_order_cancel" << body;

        return retval;
    }

    document write_operation(const feed_publish_operation& op)  {
        document retval;
        document body;

        body << "publisher"  << op.publisher
             << "exchange_rate" << std::to_string(op.exchange_rate.to_real());

        retval << "feed_publish" << body;

        return retval;
    }

    document write_operation(const convert_operation& op) {
        document retval;
        document body;

        body << "owner"  << op.owner
             << "requestid" << std::to_string(op.requestid)
             << "amount" << op.amount.to_string();

        retval << "convert" << body;

        return retval;
    }

    document write_operation(const account_create_operation& op) {
        document retval;
        document body;

        body << "fee"  << op.fee.to_string()
             << "creator" << op.creator
             << "new_account_name" << op.new_account_name;
        body << "owner" << format_authority(op.owner);
        body << "json_metadata" << op.json_metadata
             << "memo_key" << (std::string)op.memo_key;
        body << "posting" << format_authority(op.posting);

        retval << "account_create" << body;

        return retval;
    }
    document write_operation(const account_update_operation& op)  {
        document retval;
        document body;

        document owner_doc;
        if (op.owner) {
            owner_doc = format_authority(*op.owner);
        }

        document posting_owner_doc;
        if (op.posting) {
            owner_doc = format_authority(*op.posting);
        }

        document active_owner_doc;
        if (op.active) {
            owner_doc = format_authority(*op.active);
        }


        body << "account" << op.account;
        if (op.owner) {
            body << "owner" << owner_doc;
        }
        if (op.posting) {
            body << "owner" << posting_owner_doc;
        }
        if (op.active) {
            body << "active" << active_owner_doc;
        }
        body << "json_metadata" << op.json_metadata
             << "memo_key" << (std::string)op.memo_key;


        retval << "account_update" << body;

        return retval;
    }

    document write_operation(const witness_update_operation& op) {
        document retval;
        document body;

        body << "owner"  << op.owner
             << "fee" << op.fee.to_string()
             << "url" << op.url
             << "block_signing_key" << (std::string)op.block_signing_key;
        body << "props" << format_chain_properties(op.props);

        retval << "witness_update" << body;

        return retval;
    }

    document write_operation(const account_witness_vote_operation& op) {
        document retval;
        document body;

        body << "account"  << op.account
             << "witness" << op.witness
             << "approve" << op.approve;

        retval << "account_witness_vote" << body;

        return retval;
    }
    document write_operation(const account_witness_proxy_operation& op) {
        document retval;
        document body;

        body << "account"  << op.account
             << "witness" << op.proxy;

        retval << "account_witness_proxy" << body;

        return retval;
    }

    document write_operation(const pow_operation& op) {
        document retval;
        document body;

        document pow_doc;
        pow_doc << "worker" << (std::string)op.work.worker
                << "input" << op.work.input
                << "signature" << to_string(op.work.signature)
                << "work" << op.work.work;

        body << "block_id" << op.block_id.str()
             << "worker_account" << op.worker_account
             << "nonce" << std::to_string(op.nonce);
        body << "props" << format_chain_properties(op.props);
        body << "work" << pow_doc;

        retval << "pow" << body;

        return retval;
    }
    document write_operation(const custom_operation& op) {
        document retval;
        document body;

        array auths;
        for (auto iter : op.required_auths) {
            auths << iter;
        }
        body << "id" << std::to_string(op.id);
        body << "required_auths" << auths;

        retval << "custom" << body;

        return retval;
    }
    document write_operation(const report_over_production_operation& op) {
        document retval;
        document body;

        document doc1;
        doc1 << "id" << op.first_block.id().str()
             << "timestamp" << op.first_block.timestamp
             << "witness" << op.first_block.witness;

        document doc2;
        doc2 << "id" << op.second_block.id().str()
             << "timestamp" << op.second_block.timestamp
             << "witness" << op.second_block.witness;

        body << "reporter" << op.reporter;
        body << "first_block" << doc1;
        body << "second_block" << doc2;

        retval << "report_over_production" << body;

        return retval;
    }

    document write_operation(const delete_comment_operation& op){
        document retval;
        document body;

        body << "author" << op.author
             << "permlink" << op.permlink;

        retval << "delete_comment" << body;

        return retval;
    }
    document write_operation(const custom_json_operation& op) {
        document retval;
        document body;

        body << "id" << op.id
             << "json" << op.json;

        retval << "custom_json" << body;

        return retval;
    }

    document write_operation(const comment_options_operation& op) {
        document retval;
        document body;

        body << "author" << op.author
             << "permlink" << op.permlink
             << "max_accepted_payout" << op.max_accepted_payout.to_string()
             << "percent_steem_dollars" << std::to_string(op.percent_steem_dollars)
             << "allow_votes" << (op.allow_votes ? std::string("true") : std::string("false"))
             << "allow_curation_rewards" << (op.allow_curation_rewards ? std::string("true") : std::string("false"));

        retval << "comment_options" << body;

        return retval;
    }

    document write_operation(const set_withdraw_vesting_route_operation& op) {
        document retval;
        document body;

        body << "from_account" << op.from_account
             << "to_account" << op.to_account
             << "percent" << std::to_string(op.percent)
             << "auto_vest" << (op.auto_vest ? std::string("true") : std::string("false"));

        retval << "set_withdraw_vesting_route" << body;

        return retval;
    }

    document write_operation(const limit_order_create2_operation& op) {
        document retval;
        document body;

        body << "owner" << op.owner
             << "orderid" << std::to_string(op.orderid)
             << "amount_to_sell" << op.amount_to_sell.to_string()
             << "fill_or_kill" << (op.fill_or_kill ? std::string("true") : std::string("false"))
             << "exchange_rate" << std::to_string(op.exchange_rate.to_real())
             << "expiration" << op.expiration;


        retval << "limit_order_create2" << body;

        return retval;
    }
    document write_operation(const challenge_authority_operation& op) {
        document retval;
        document body;

        body << "challenger" << op.challenger
             << "challenged" << op.challenged
             << "require_owner" << (op.require_owner ? std::string("true") : std::string("false"));

        retval << "challenge_authority" << body;

        return retval;
    }
    document write_operation(const prove_authority_operation& op){
        document retval;
        document body;

        body << "challenged" << op.challenged
             << "require_owner" << (op.require_owner ? std::string("true") : std::string("false"));

        retval << "prove_authority" << body;

        return retval;
    }

    document write_operation(const request_account_recovery_operation& op) {
        document retval;
        document body;

        body << "recovery_account" << op.recovery_account
             << "account_to_recover" << op.account_to_recover;
        body << "new_owner_authority" << format_authority(op.new_owner_authority);

        retval << "request_account_recovery" << body;

        return retval;
    }

    document write_operation(const recover_account_operation& op){
        document retval;
        document body;

        body << "account_to_recover" << op.account_to_recover;
        body << "new_owner_authority" << format_authority(op.new_owner_authority);
        body << "recent_owner_authority" << format_authority(op.recent_owner_authority);

        retval << "recover_account" << body;

        return retval;
    }

    document write_operation(const change_recovery_account_operation& op){
        document retval;
        document body;

        body << "account_to_recover" << op.account_to_recover
             << "new_recovery_account" << op.new_recovery_account;

        retval << "change_recovery_account" << body;

        return retval;
    }

    document write_operation(const escrow_transfer_operation& op) {
        document retval;
        document body;

        body << "from" << op.from
             << "to" << op.to
             << "agent" << op.agent
             << "escrow_id" << std::to_string(op.escrow_id)
             << "sbd_amount" << op.sbd_amount.to_string()
             << "steem_amount " << op.steem_amount.to_string()
             << "fee" << op.fee.to_string()
             << "json_meta" << op.json_meta;

        retval << "escrow_transfer" << body;

        return retval;
    }

    document write_operation(const escrow_dispute_operation& op)  {
        document retval;
        document body;

        body << "from" << op.from
             << "to" << op.to
             << "agent" << op.agent
             << "who" << op.who
             << "escrow_id" << std::to_string(op.escrow_id);

        retval << "escrow_dispute" << body;

        return retval;
    }
    document write_operation(const escrow_release_operation& op) {
        document retval;
        document body;

        body << "from" << op.from
             << "to" << op.to
             << "agent" << op.agent
             << "who" << op.who
             << "receiver" << op.receiver
             << "escrow_id" << std::to_string(op.escrow_id)
             << "sbd_amount" << op.sbd_amount.to_string()
             << "steem_amount" << op.steem_amount.to_string();

        retval << "escrow_release" << body;

        return retval;
    }

    document write_operation(const pow2_operation& op) {
        document retval;
        document body;

        body << "props" << format_chain_properties(op.props);
        if (op.new_owner_key) {
            body << "new_owner_key" << (std::string)(*op.new_owner_key);
        }

        retval << "pow2" << body;

        return retval;
    }

    document write_operation(const escrow_approve_operation& op) {
        document retval;
        document body;

        body << "from" << op.from
             << "to" << op.to
             << "agent" << op.agent
             << "who" << op.who
             << "escrow_id" << std::to_string(op.escrow_id)
             << "approve" << (op.approve ? std::string("true") : std::string("false"));

        retval << "escrow_approve" << body;

        return retval;
    }

    document write_operation(const transfer_to_savings_operation& op){
        document retval;
        document body;

        body << "from" << op.from
             << "to" << op.to
             << "amount" << op.amount.to_string()
             << "memo" << op.memo;

        retval << "transfer_to_savings" << body;

        return retval;
    }

    document write_operation(const transfer_from_savings_operation& op) {
        document retval;
        document body;

        body << "from" << op.from
             << "to" << op.to
             << "amount" << op.amount.to_string()
             << "memo" << op.memo
             << "request_id" << std::to_string(op.request_id);

        retval << "transfer_from_savings" << body;

        return retval;
    }
    document write_operation(const cancel_transfer_from_savings_operation& op) {
        document retval;
        document body;

        body << "from" << op.from
             << "request_id" << std::to_string(op.request_id);

        retval << "cancel_transfer_from_savings" << body;

        return retval;
    }

    document write_operation(const custom_binary_operation& op){
        document retval;
        document body;

        array required_owner_auths_arr;
        for (auto iter : op.required_owner_auths) {
            required_owner_auths_arr << iter;
        }

        array required_active_auths_arr;
        for (auto iter : op.required_active_auths) {
            required_active_auths_arr << iter;
        }

        array required_posting_auths_arr;
        for (auto iter : op.required_posting_auths) {
            required_posting_auths_arr << iter;
        }

        array auths;
        for (auto iter : op.required_auths) {
            auths << format_authority(iter);
        }

        body << "id" << op.id;
        body << "required_owner_auths" << required_owner_auths_arr;
        body << "required_active_auths" << required_active_auths_arr;
        body << "required_posting_auths" << required_posting_auths_arr;
        body << "required_auths" << auths;

        retval << "custom_binary" << body;

        return retval;
    }

    document write_operation(const decline_voting_rights_operation& op) {
        document retval;
        document body;

        body << "account" << op.account
             << "decline" << (op.decline ? std::string("true") : std::string("false"));

        retval << "decline_voting_rights" << body;

        return retval;
    }

    document write_operation(const reset_account_operation& op){
        document retval;
        document body;

        body << "reset_account" << op.reset_account
             << "account_to_reset" << op.account_to_reset;
        body << "new_owner_authority" << format_authority(op.new_owner_authority);

        retval << "reset_account" << body;

        return retval;
    }

    document write_operation(const set_reset_account_operation& op){
        document retval;
        document body;

        body << "account" << op.account
             << "current_reset_account" << op.current_reset_account
             << "reset_account" << op.reset_account;

        retval << "set_reset_account" << body;

        return retval;
    }

    document write_operation(const fill_convert_request_operation& op) {
        document retval;
        document body;

        body << "owner" << op.owner
             << "requestid" << std::to_string(op.requestid)
             << "amount_in" << op.amount_in.to_string()
             << "amount_out" << op.amount_out.to_string();

        retval << "fill_convert_request" << body;

        return retval;
    }

    document write_operation(const author_reward_operation& op)  {
        document retval;
        document body;

        body << "author" << op.author
             << "permlink" << op.permlink
             << "sbd_payout" << op.sbd_payout.to_string()
             << "steem_payout" << op.steem_payout.to_string()
             << "vesting_payout" << op.vesting_payout.to_string();

        retval << "author_reward" << body;

        return retval;
    }

    document write_operation(const curation_reward_operation& op){
        document retval;
        document body;

        body << "curator" << op.curator
             << "reward" << op.reward.to_string()
             << "comment_author" << op.comment_author
             << "comment_permlink" << op.comment_permlink;

        retval << "curation_reward" << body;

        return retval;
    }

    document write_operation(const comment_reward_operation& op){
        document retval;
        document body;

        body << "author" << op.author
             << "permlink" << op.permlink
             << "payout" << op.payout.to_string();

        retval << "comment_reward" << body;

        return retval;
    }

    document write_operation(const liquidity_reward_operation& op){
        document retval;
        document body;

        body << "owner" << op.owner
             << "payout" << op.payout.to_string();

        retval << "liquidity_reward" << body;

        return retval;
    }

    document write_operation(const interest_operation& op){
        document retval;
        document body;

        body << "owner" << op.owner
             << "interest" << op.interest.to_string();

        retval << "interest" << body;

        return retval;
    }

    document write_operation(const fill_vesting_withdraw_operation& op) {
        document retval;
        document body;

        body << "from_account" << op.from_account
             << "to_account" << op.to_account
             << "withdrawn" << op.withdrawn.to_string()
             << "deposited" << op.deposited.to_string();

        retval << "fill_vesting_withdraw" << body;

        return retval;
    }

    document write_operation(const fill_order_operation& op) {
        document retval;
        document body;

        body << "current_owner" << op.current_owner
             << "current_orderid" << std::to_string(op.current_orderid)
             << "current_pays" << op.current_pays.to_string()
             << "open_owner" << op.open_owner
             << "open_orderid" << std::to_string(op.open_orderid)
             << "open_pays" << op.open_pays.to_string();

        retval << "fill_order" << body;

        return retval;
    }

    document write_operation(const shutdown_witness_operation& op){
        document retval;
        document body;

        body << "owner" << op.owner;

        retval << "shutdown_witness" << body;

        return retval;
    }

    document write_operation(const fill_transfer_from_savings_operation& op){
        document retval;
        document body;

        body << "from" << op.from
             << "to" << op.to
             << "amount" << op.amount.to_string()
             << "request_id" << std::to_string(op.request_id)
             << "memo" << op.memo;

        retval << "fill_transfer_from_savings" << body;

        return retval;
    }

    document write_operation(const hardfork_operation& op) {
        document retval;
        document body;

        body << "hardfork_id" << std::to_string(op.hardfork_id);

        retval << "hardfork" << body;

        return retval;
    }

    document write_operation(const comment_payout_update_operation& op){
        document retval;
        document body;

        body << "author" << op.author
             << "permlink" << op.permlink;

        retval << "comment_payout_update" << body;

        return retval;
    }

}}}
