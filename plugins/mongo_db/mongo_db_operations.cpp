#include <golos/plugins/mongo_db/mongo_db_operations.hpp>

#include <bsoncxx/builder/stream/array.hpp>
#include <bsoncxx/builder/stream/value_context.hpp>
#include <bsoncxx/builder/basic/document.hpp>

namespace golos {
namespace plugins {
namespace mongo_db {

    using bsoncxx::builder::stream::array;
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

    document format_asset(const asset& _asset) {
        document asset_doc;
        std::string value = std::to_string(_asset.to_real());

        asset_doc << "amount" << (value.empty() ? "0" : value)
                  << "symbol_name" << (_asset.symbol_name().empty() ? "_" : _asset.symbol_name());

        return asset_doc;
    }

    std::string to_string(const signature_type& signature) {
        std::string retVal;

        for (auto iter : signature) {
            retVal += iter;
        }
        return retVal;
    }
    /////////////////////////////////////////////////
    document& operation_writer::get_document() {
        return data;
    }

    void operation_writer::log_operation(const std::string& name) {
        ilog("MongoDB operation: ${p}", ("p", name));
    }

    void operation_writer::format_value(document& doc, const std::string& name, const std::string& value) {
        if (name.empty() || value.empty()) {
            return;
        }
        doc << name << value;
    }

    void operation_writer::operator()(const vote_operation &op) {
        document body;

        log_operation("vote");

        format_value(body, "voter", op.voter);
        format_value(body, "author", op.author);
        format_value(body, "permlink", op.permlink);
        format_value(body, "weight", std::to_string(op.weight));

        data << "vote" << body;
    }

    void operation_writer::operator()(const comment_operation &op) {
        document body;

        log_operation("comment");

        format_value(body, "parent_author", op.parent_author);
        format_value(body, "parent_permlink", op.parent_permlink);
        format_value(body, "author", op.author);
        format_value(body, "permlink", op.permlink);
        format_value(body, "title", op.title);
        format_value(body, "body", op.body);
        format_value(body, "json_metadata", op.json_metadata);

        data << "comment" << body;
    }

    void operation_writer::operator()(const transfer_operation &op) {
        document body;

        log_operation("transfer");

        format_value(body, "from", op.from);
        format_value(body, "to", op.to);
        body << "amount" << format_asset(op.amount);
        format_value(body, "memo", op.memo);

        data << "transfer" << body;
    }

    void operation_writer::operator()(const transfer_to_vesting_operation &op) {
        document body;

        log_operation("transfer_to_vesting");

        format_value(body, "from", op.from);
        format_value(body, "to", op.to);
        body << "amount" << format_asset(op.amount);

        data << "transfer_to_vesting" << body;
    }

    void operation_writer::operator()(const withdraw_vesting_operation &op) {
        document body;

        log_operation("withdraw_vesting");

        format_value(body, "account", op.account);
        body << "vesting_shares" << format_asset(op.vesting_shares);

        data << "withdraw_vesting" << body;
    }

    void operation_writer::operator()(const limit_order_create_operation &op) {
        document body;

        log_operation("limit_order_create");

        format_value(body, "owner", op.owner);
        format_value(body, "orderid", std::to_string(op.orderid));
        body << "amount_to_sell" << format_asset(op.amount_to_sell);
        body << "min_to_receive" << format_asset(op.min_to_receive);
        format_value(body, "expiration", op.expiration);

        body << "limit_order_create" << body;
    }

    void operation_writer::operator()(const limit_order_cancel_operation &op) {
        document body;

        log_operation("limit_order_cancel");

        format_value(body, "owner", op.owner);
        format_value(body, "orderid", std::to_string(op.orderid));

        data << "limit_order_cancel" << body;
    }

    void operation_writer::operator()(const feed_publish_operation &op) {
        document body;

        log_operation("feed_publish");

        format_value(body, "publisher", op.publisher);
        format_value(body, "exchange_rate", std::to_string(op.exchange_rate.to_real()));

        data << "feed_publish" << body;
    }

    void operation_writer::operator()(const convert_operation &op) {
        document body;

        log_operation("convert");

        format_value(body, "owner", op.owner);
        format_value(body, "requestid", std::to_string(op.requestid));
        body << "amount" << format_asset(op.amount);

        data << "convert" << body;
    }

    void operation_writer::operator()(const account_create_operation &op) {
        document body;

        log_operation("account_create");

        format_value(body, "fee", op.fee.to_string());
        format_value(body, "creator", op.creator);
        format_value(body, "new_account_name", op.new_account_name);
        body << "owner" << format_authority(op.owner);
        format_value(body, "json_metadata", op.json_metadata);
        format_value(body, "memo_key", (std::string)op.memo_key);
        body << "posting" << format_authority(op.posting);

        data << "account_create" << body;
    }

    void operation_writer::operator()(const account_update_operation &op) {
        document body;

        log_operation("account_update");

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

        format_value(body, "account", op.account);
        if (op.owner) {
            body << "owner" << owner_doc;
        }
        if (op.posting) {
            body << "owner" << posting_owner_doc;
        }
        if (op.active) {
            body << "active" << active_owner_doc;
        }
        format_value(body, "json_metadata", op.json_metadata);
        format_value(body, "memo_key", (std::string)op.memo_key);

        data << "account_update" << body;
    }

    void operation_writer::operator()(const witness_update_operation &op) {
        document body;

        log_operation("witness_update");

        format_value(body, "owner", op.owner);
        format_value(body, "fee", op.fee.to_string());
        format_value(body, "url", op.url);
        format_value(body, "block_signing_key", (std::string)op.block_signing_key);
        body << "props" << format_chain_properties(op.props);

        data << "witness_update" << body;
    }

    void operation_writer::operator()(const account_witness_vote_operation &op) {
        document body;

        log_operation("account_witness_vote");

        format_value(body, "account", op.account);
        format_value(body, "witness", op.witness);
        format_value(body, "approve", (op.approve ? std::string("true") : std::string("false")));

        data << "account_witness_vote" << body;
    }

    void operation_writer::operator()(const account_witness_proxy_operation &op) {
        document body;

        log_operation("account_witness_proxy");

        format_value(body, "account", op.account);
        format_value(body, "proxy", op.proxy);

        data << "account_witness_proxy" << body;
    }

    void operation_writer::operator()(const pow_operation &op) {
        document body;

        log_operation("pow");

        document pow_doc;
        format_value(pow_doc, "worker", (std::string)op.work.worker);
        format_value(pow_doc, "input", op.work.input);
        format_value(pow_doc, "signature", to_string(op.work.signature));
        format_value(pow_doc, "work", op.work.work);

        format_value(body, "block_id", op.block_id.str());
        format_value(body, "worker_account", op.worker_account);
        format_value(body, "nonce", std::to_string(op.nonce));

        body << "props" << format_chain_properties(op.props);
        body << "work" << pow_doc;

        data << "pow" << body;
    }

    void operation_writer::operator()(const custom_operation &op) {
        document body;

        log_operation("custom");

        format_value(body, "id", std::to_string(op.id));
        if (!op.required_auths.empty()) {
            array auths;
            for (auto iter : op.required_auths) {
                auths << iter;
            }
            body << "required_auths" << auths;
        }

        data << "custom" << body;
    }

    void operation_writer::operator()(const report_over_production_operation &op) {
        document body;

        log_operation("report_over_production");

        document doc1;
        format_value(doc1, "id", op.first_block.id().str());
        format_value(doc1, "timestamp", op.first_block.timestamp);
        format_value(doc1, "witness", op.first_block.witness);

        document doc2;
        format_value(doc2, "id", op.second_block.id().str());
        format_value(doc2, "timestamp", op.first_block.timestamp);
        format_value(doc2, "witness", op.second_block.witness);

        format_value(body, "reporter", op.reporter);

        body << "first_block" << doc1;
        body << "second_block" << doc2;

        data << "report_over_production" << body;
    }

    void operation_writer::operator()(const delete_comment_operation &op) {
        document body;

        log_operation("delete_comment");

        format_value(body, "author", op.author);
        format_value(body, "permlink", op.permlink);

        data << "delete_comment" << body;
    }

    void operation_writer::operator()(const custom_json_operation &op) {
        document body;

        log_operation("custom_json");

        format_value(body, "id", op.id);
        format_value(body, "json", op.json);

        data << "custom_json" << body;
    }

    void operation_writer::operator()(const comment_options_operation &op) {
        document body;

        log_operation("comment_options");

        format_value(body, "author", op.author);
        format_value(body, "permlink", op.permlink);
        format_value(body, "max_accepted_payout", op.max_accepted_payout.to_string());
        format_value(body, "percent_steem_dollars", std::to_string(op.percent_steem_dollars));
        format_value(body, "allow_votes", (op.allow_votes ? std::string("true") : std::string("false")));
        format_value(body, "allow_curation_rewards", (op.allow_curation_rewards ? std::string("true") : std::string("false")));

        data << "comment_options" << body;
    }

    void operation_writer::operator()(const set_withdraw_vesting_route_operation &op) {
        document body;

        log_operation("set_withdraw_vesting_route");

        format_value(body, "from_account", op.from_account);
        format_value(body, "to_account", op.to_account);
        format_value(body, "percent", std::to_string(op.percent));
        format_value(body, "auto_vest", (op.auto_vest ? std::string("true") : std::string("false")));

        data << "set_withdraw_vesting_route" << body;
    }

    void operation_writer::operator()(const limit_order_create2_operation &op) {
        document body;

        log_operation("limit_order_create2");

        format_value(body, "owner", op.owner);
        format_value(body, "orderid", std::to_string(op.orderid));
        format_value(body, "amount_to_sell", op.amount_to_sell.to_string());
        format_value(body, "fill_or_kill", (op.fill_or_kill ? std::string("true") : std::string("false")));
        format_value(body, "exchange_rate", std::to_string(op.exchange_rate.to_real()));
        format_value(body, "expiration", op.expiration);

        data << "limit_order_create2" << body;
    }

    void operation_writer::operator()(const challenge_authority_operation &op) {
        document body;

        log_operation("challenge_authority");

        format_value(body, "challenger", op.challenger);
        format_value(body, "challenged", op.challenged);
        format_value(body, "require_owner", (op.require_owner ? std::string("true") : std::string("false")));

        data << "challenge_authority" << body;
    }

    void operation_writer::operator()(const prove_authority_operation &op) {
        document body;

        log_operation("prove_authority");

        format_value(body, "challenged", op.challenged);
        format_value(body, "require_owner", (op.require_owner ? std::string("true") : std::string("false")));

        data << "prove_authority" << body;
    }

    void operation_writer::operator()(const request_account_recovery_operation &op) {
        document body;

        log_operation("request_account_recovery");

        format_value(body, "recovery_account", op.recovery_account);
        format_value(body, "account_to_recover", op.account_to_recover);
        body << "new_owner_authority" << format_authority(op.new_owner_authority);

        data << "request_account_recovery" << body;
    }

    void operation_writer::operator()(const recover_account_operation &op) {
        document body;

        log_operation("recover_account");

        format_value(body, "account_to_recover", op.account_to_recover);
        body << "new_owner_authority" << format_authority(op.new_owner_authority);
        body << "recent_owner_authority" << format_authority(op.recent_owner_authority);

        data << "recover_account" << body;
    }

    void operation_writer::operator()(const change_recovery_account_operation &op) {
        document body;

        log_operation("change_recovery_account");

        format_value(body, "account_to_recover", op.account_to_recover);
        format_value(body, "new_recovery_account", op.new_recovery_account);

        data << "change_recovery_account" << body;
    }

    void operation_writer::operator()(const escrow_transfer_operation &op) {
        document body;

        log_operation("escrow_transfer");

        format_value(body, "from", op.from);
        format_value(body, "to", op.to);
        format_value(body, "agent", op.agent);
        format_value(body, "escrow_id", std::to_string(op.escrow_id));

        body << "sbd_amount" << format_asset(op.sbd_amount);
        body << "steem_amount " << format_asset(op.steem_amount);
        body << "fee" << format_asset(op.fee);
        format_value(body, "json_meta", op.json_meta);

        data << "escrow_transfer" << body;
    }

    void operation_writer::operator()(const escrow_dispute_operation &op) {
        document body;

        log_operation("escrow_dispute");

        format_value(body, "from", op.from);
        format_value(body, "to", op.to);
        format_value(body, "agent", op.agent);
        format_value(body, "who", op.who);
        format_value(body, "escrow_id", std::to_string(op.escrow_id));

        data << "escrow_dispute" << body;
    }

    void operation_writer::operator()(const escrow_release_operation &op) {
        document body;

        log_operation("escrow_release");

        format_value(body, "from", op.from);
        format_value(body, "to", op.to);
        format_value(body, "agent", op.agent);
        format_value(body, "who", op.who);
        format_value(body, "receiver", op.receiver);
        format_value(body, "escrow_id", std::to_string(op.escrow_id));

        body << "sbd_amount" << format_asset(op.sbd_amount);
        body << "steem_amount" << format_asset(op.steem_amount);

        data << "escrow_release" << body;
    }

    void operation_writer::operator()(const pow2_operation &op) {
        document body;

        log_operation("pow2");

        body << "props" << format_chain_properties(op.props);
        if (op.new_owner_key) {
            format_value(body, "new_owner_key", (std::string)(*op.new_owner_key));
        }

        data << "pow2" << body;
    }

    void operation_writer::operator()(const escrow_approve_operation &op) {
        document body;

        log_operation("escrow_approve");

        format_value(body, "from", op.from);
        format_value(body, "to", op.to);
        format_value(body, "agent", op.agent);
        format_value(body, "who", op.who);
        format_value(body, "escrow_id", std::to_string(op.escrow_id));
        format_value(body, "approve", (op.approve ? std::string("true") : std::string("false")));

        data << "escrow_approve" << body;
    }

    void operation_writer::operator()(const transfer_to_savings_operation &op) {
        document body;

        log_operation("transfer_to_savings");

        format_value(body, "from", op.from);
        format_value(body, "to", op.to);
        body << "amount" << format_asset(op.amount);
        format_value(body, "memo", op.memo);

        data << "transfer_to_savings" << body;
    }

    void operation_writer::operator()(const transfer_from_savings_operation &op) {
        document body;

        log_operation("transfer_from_savings");

        format_value(body, "from", op.from);
        format_value(body, "to", op.to);
        body << "amount" << format_asset(op.amount);
        format_value(body, "memo", op.memo);
        format_value(body, "request_id", std::to_string(op.request_id));

        data << "transfer_from_savings" << body;
    }

    void operation_writer::operator()(const cancel_transfer_from_savings_operation &op) {
        document body;

        log_operation("cancel_transfer_from_savings");

        format_value(body, "from", op.from);
        format_value(body, "request_id", std::to_string(op.request_id));

        data << "cancel_transfer_from_savings" << body;
    }

    void operation_writer::operator()(const custom_binary_operation &op) {
        document body;

        log_operation("custom_binary");

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

        format_value(body, "id", op.id);
        body << "required_owner_auths" << required_owner_auths_arr;
        body << "required_active_auths" << required_active_auths_arr;
        body << "required_posting_auths" << required_posting_auths_arr;
        body << "required_auths" << auths;

        data << "custom_binary" << body;
    }

    void operation_writer::operator()(const decline_voting_rights_operation &op) {
        document body;

        log_operation("decline_voting_rights");

        format_value(body, "account", op.account);
        format_value(body, "decline", (op.decline ? std::string("true") : std::string("false")));

        data << "decline_voting_rights" << body;
    }

    void operation_writer::operator()(const reset_account_operation &op) {
        document body;

        log_operation("reset_account");

        format_value(body, "reset_account", op.reset_account);
        format_value(body, "account_to_reset", op.account_to_reset);
        body << "new_owner_authority" << format_authority(op.new_owner_authority);

        data << "reset_account" << body;
    }

    void operation_writer::operator()(const set_reset_account_operation &op) {
        document body;

        log_operation("set_reset_account");

        format_value(body, "account", op.account);
        format_value(body, "current_reset_account", op.current_reset_account);
        format_value(body, "reset_account", op.reset_account);

        data << "set_reset_account" << body;
    }

    void operation_writer::operator()(const fill_convert_request_operation &op) {
        document body;

        log_operation("fill_convert_request");

        format_value(body, "owner", op.owner);
        format_value(body, "requestid", std::to_string(op.requestid));
        body << "amount_in" << format_asset(op.amount_in);
        body << "amount_out" << format_asset(op.amount_out);

        data << "fill_convert_request" << body;
    }

    void operation_writer::operator()(const author_reward_operation &op) {
        document body;

        log_operation("author_reward");

        format_value(body, "author", op.author);
        format_value(body, "permlink", op.permlink);
        body << "sbd_payout" << format_asset(op.sbd_payout);
        body << "steem_payout" << format_asset(op.steem_payout);
        body << "vesting_payout" << format_asset(op.vesting_payout);

        data << "author_reward" << body;
    }

    void operation_writer::operator()(const curation_reward_operation &op) {
        document body;

        log_operation("curation_reward");

        format_value(body, "curator", op.curator);
        body << "reward" << format_asset(op.reward);
        format_value(body, "comment_author", op.comment_author);
        format_value(body, "comment_permlink", op.comment_permlink);

        data << "curation_reward" << body;
    }

    void operation_writer::operator()(const comment_reward_operation &op) {
        document body;

        log_operation("comment_reward");

        format_value(body, "author", op.author);
        format_value(body, "permlink", op.permlink);
        body << "payout" << format_asset(op.payout);

        data << "comment_reward" << body;
    }

    void operation_writer::operator()(const liquidity_reward_operation &op) {
        document body;

        log_operation("liquidity_reward");

        format_value(body, "owner", op.owner);
        body << "payout" << format_asset(op.payout);

        data << "liquidity_reward" << body;
    }

    void operation_writer::operator()(const interest_operation &op) {
        document body;

        log_operation("interest");

        format_value(body, "owner", op.owner);
        body << "interest" << format_asset(op.interest);

        data << "interest" << body;
    }

    void operation_writer::operator()(const fill_vesting_withdraw_operation &op) {
        document body;

        log_operation("fill_vesting_withdraw");

        format_value(body, "from_account", op.from_account);
        format_value(body, "to_account", op.to_account);
        body << "withdrawn" << format_asset(op.withdrawn);
        body << "deposited" << format_asset(op.deposited);

        data << "fill_vesting_withdraw" << body;
    }

    void operation_writer::operator()(const fill_order_operation &op) {
        document body;

        log_operation("fill_order");

        format_value(body, "current_owner", op.current_owner);
        format_value(body, "current_orderid", std::to_string(op.current_orderid));
        body << "current_pays" << format_asset(op.current_pays);
        format_value(body, "open_owner", op.open_owner);
        format_value(body, "open_orderid", std::to_string(op.open_orderid));
        body << "open_pays" << format_asset(op.open_pays);

        data << "fill_order" << body;
    }

    void operation_writer::operator()(const shutdown_witness_operation &op) {
        document body;

        log_operation("shutdown_witness");

        format_value(body, "owner", op.owner);

        data << "shutdown_witness" << body;
    }

    void operation_writer::operator()(const fill_transfer_from_savings_operation &op) {
        document body;

        log_operation("fill_transfer_from_savings");

        format_value(body, "from", op.from);
        format_value(body, "to", op.to);
        body << "amount" << format_asset(op.amount);
        format_value(body, "request_id", std::to_string(op.request_id));
        format_value(body, "memo", op.memo);

        data << "fill_transfer_from_savings" << body;
    }

    void operation_writer::operator()(const hardfork_operation &op) {
        document body;

        log_operation("hardfork");

        format_value(body, "hardfork_id", std::to_string(op.hardfork_id));

        data << "hardfork" << body;
    }

    void operation_writer::operator()(const comment_payout_update_operation &op) {
        document body;

        log_operation("comment_payout_update");

        format_value(body, "author", op.author);
        format_value(body, "permlink", op.permlink);

        body << "comment_payout_update" << body;
    }

    void operation_writer::operator()(const comment_benefactor_reward_operation& op) {
        document body;

        log_operation("comment_benefactor_reward_operation");

        format_value(body, "benefactor", op.benefactor);
        format_value(body, "author", op.author);
        format_value(body, "permlink", op.permlink);
        body << "reward" << format_asset(op.reward);

        body << "comment_benefactor_reward_operation" << body;
    }

}}}
