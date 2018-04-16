#include <golos/plugins/mongo_db/mongo_db_operations.hpp>
#include <golos/plugins/follow/follow_objects.hpp>
#include <golos/plugins/chain/plugin.hpp>
#include <golos/chain/comment_object.hpp>
#include <golos/chain/account_object.hpp>

#include <bsoncxx/builder/stream/array.hpp>
#include <bsoncxx/builder/stream/value_context.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <appbase/plugin.hpp>


namespace golos {
namespace plugins {
namespace mongo_db {

    using bsoncxx::builder::stream::array;
    using bsoncxx::builder::stream::document;
    using namespace golos::plugins::follow;

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

    void format_asset(const asset& _asset, document& asset_doc, const std::string& val) {
        array arr;
        arr << std::to_string(_asset.to_real());
        arr << _asset.symbol_name();
        asset_doc << val << arr;
    }

    void format_value(document& doc, const std::string& name, const std::string& value) {
        if (name.empty() || value.empty()) {
            return;
        }
        doc << name << value;
    }

    std::string to_string(const signature_type& signature) {
        std::string retVal;

        for (auto iter : signature) {
            retVal += iter;
        }
        return retVal;
    }

    /////////////////////////////////////////////////

    operation_writer::operation_writer() :
            _db(appbase::app().get_plugin<golos::plugins::chain::plugin>().db()) {
    }

    document& operation_writer::get_document() {
        return data;
    }

    void operation_writer::log_operation(const std::string& name) {
        //ilog("MongoDB operation: ${p}", ("p", name));
    }

    void operation_writer::format_comment(const std::string& auth, const std::string& perm, document& comment_doc) {

        const comment_object* comment_obj_ptr;

        _db.with_read_lock([&](){
            comment_obj_ptr =_db.find_comment(auth, perm);

            if (comment_obj_ptr == NULL) {
                return;
            }
        });

        comment_object comment_obj = *comment_obj_ptr;

        format_value(comment_doc, "author", comment_obj.author);
        format_value(comment_doc, "permlink", comment_obj.permlink.c_str());
        format_value(comment_doc, "abs_rshares", comment_obj.abs_rshares);
        format_value(comment_doc, "active", comment_obj.active.to_iso_string());
        format_comment_active_votes(comment_obj, comment_doc);
        format_value(comment_doc, "allow_curation_rewards", (comment_obj.allow_curation_rewards ? "true" : "false"));
        format_value(comment_doc, "allow_replies", (comment_obj.allow_replies ? "true" : "false"));
        format_value(comment_doc, "allow_votes", (comment_obj.allow_votes ? "true" : "false"));
        format_value(comment_doc, "author_rewards", comment_obj.author_rewards);
        format_value(comment_doc, "author_reputation", get_account_reputation(comment_obj.author));
        format_value(comment_doc, "body", comment_obj.body.c_str());
        format_value(comment_doc, "body_length", std::to_string(comment_obj.body.length()));
        format_value(comment_doc, "cashout_time", comment_obj.cashout_time.to_iso_string());
        format_value(comment_doc, "category", comment_obj.category.c_str());
        format_value(comment_doc, "children", std::to_string(comment_obj.children));
        format_value(comment_doc, "children_abs_rshares", std::to_string(comment_obj.children_abs_rshares.value));
        format_value(comment_doc, "children_rshares2", comment_obj.children_rshares2);
        format_value(comment_doc, "created", comment_obj.created.to_iso_string());
        format_asset(comment_obj.curator_payout_value, comment_doc, "curator_payout_value");
        format_value(comment_doc, "depth", std::to_string(comment_obj.depth));
        format_value(comment_doc, "id", std::to_string(comment_obj.id._id));
        format_value(comment_doc, "last_payout", comment_obj.last_payout.to_iso_string());
        format_value(comment_doc, "last_update", comment_obj.last_update.to_iso_string());
        format_asset(comment_obj.max_accepted_payout, comment_doc, "max_accepted_payout");
        format_value(comment_doc, "max_cashout_time", comment_obj.max_cashout_time.to_iso_string());

        std::string comment_mode;
        switch (comment_obj.mode) {
            case first_payout:
                comment_mode = "first_payout";
                break;
            case second_payout:
                comment_mode = "second_payout";
                break;
            case archived:
                comment_mode = "archived";
                break;
        }

        format_value(comment_doc, "mode", comment_mode);
        format_value(comment_doc, "net_rshares", std::to_string(comment_obj.net_rshares.value));
        format_value(comment_doc, "net_votes", std::to_string(comment_obj.net_votes));
        format_value(comment_doc, "parent_author", comment_obj.parent_author);
        format_value(comment_doc, "parent_permlink", comment_obj.parent_permlink.c_str());
        // pending_payout_value
        format_value(comment_doc, "percent_steem_dollars", std::to_string(comment_obj.percent_steem_dollars));
        // promoted
        // reblogged_by
        // replies
        format_value(comment_doc, "reward_weight", std::to_string(comment_obj.reward_weight));
        format_value(comment_doc, "root_comment", std::to_string(comment_obj.root_comment._id));
        // root_title
        // scanned
        format_value(comment_doc, "title", comment_obj.title.c_str());
        format_asset(comment_obj.total_payout_value, comment_doc, "total_payout_value");
        // total_pending_payout_value
        format_value(comment_doc, "total_vote_weight", std::to_string(comment_obj.total_vote_weight));
        // url
        format_value(comment_doc, "vote_rshares", std::to_string(comment_obj.vote_rshares.value));
        // last_reply
        // last_reply_by
    }

    void operation_writer::format_comment_active_votes(const comment_object& comment, document& doc) {

        array votes;

        const auto &idx = _db.get_index<comment_vote_index>().indices().get<by_comment_voter>();
        comment_object::id_type cid(comment.id);
        auto itr = idx.lower_bound(cid);

        while (itr != idx.end() && itr->comment == cid) {

            document vote_doc;

            const auto &vo = _db.get(itr->voter);

            format_value(vote_doc, "voter", vo.name);
            format_value(vote_doc, "weight", std::to_string(itr->weight));
            format_value(vote_doc, "rshares", std::to_string(itr->rshares));
            format_value(vote_doc, "percent", std::to_string(itr->vote_percent));
            format_value(vote_doc, "time", itr->last_update.to_iso_string());
            format_value(vote_doc, "reputation", get_account_reputation(vo.name));

            votes << vote_doc;

            ++itr;
        }

        doc << "active_votes" << votes;
    }

    std::string operation_writer::get_account_reputation(const account_name_type& account) {

        auto &rep_idx = _db.get_index<reputation_index>().indices().get<by_account>();
        auto itr = rep_idx.find(account);

        if (rep_idx.end() != itr) {
            return itr->reputation;
        }

        return "0";
    }

    void operation_writer::operator()(const vote_operation &op) {
        document body;

        log_operation("vote");

        format_comment(op.author, op.permlink, body);

        format_value(body, "voter", op.voter);
        format_value(body, "weight", std::to_string(op.weight));

        data << "vote" << body;
    }

    void operation_writer::operator()(const comment_operation &op) {
        document body;

        log_operation("comment");

        format_comment(op.author, op.permlink, body);

        format_value(body, "json_metadata", op.json_metadata);

        data << "comment" << body;
    }

    void operation_writer::operator()(const transfer_operation &op) {
        document body;

        log_operation("transfer");

        format_value(body, "from", op.from);
        format_value(body, "to", op.to);
        format_asset(op.amount, body, "amount");
        format_value(body, "memo", op.memo);

        data << "transfer" << body;
    }

    void operation_writer::operator()(const transfer_to_vesting_operation &op) {
        document body;

        log_operation("transfer_to_vesting");

        format_value(body, "from", op.from);
        format_value(body, "to", op.to);
        format_asset(op.amount, body, "amount");

        data << "transfer_to_vesting" << body;
    }

    void operation_writer::operator()(const withdraw_vesting_operation &op) {
        document body;

        log_operation("withdraw_vesting");

        format_value(body, "account", op.account);
        format_asset(op.vesting_shares, body, "vesting_shares");

        data << "withdraw_vesting" << body;
    }

    void operation_writer::operator()(const limit_order_create_operation &op) {
        document body;

        log_operation("limit_order_create");

        format_value(body, "owner", op.owner);
        format_value(body, "orderid", std::to_string(op.orderid));
        format_asset(op.amount_to_sell, body, "amount_to_sell");
        format_asset(op.min_to_receive, body, "min_to_receive");
        format_value(body, "expiration", op.expiration);

        data << "limit_order_create" << body;
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
        format_asset(op.amount, body, "amount");

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

        format_comment(op.author, op.permlink, body);

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

        format_comment(op.author, op.permlink, body);

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

        format_asset(op.sbd_amount, body, "sbd_amount");
        format_asset(op.steem_amount, body, "steem_amount ");
        format_asset(op.fee, body, "fee");
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

        format_asset(op.sbd_amount, body, "sbd_amount");
        format_asset(op.steem_amount, body, "steem_amount");

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
        format_asset(op.amount, body, "amount");
        format_value(body, "memo", op.memo);

        data << "transfer_to_savings" << body;
    }

    void operation_writer::operator()(const transfer_from_savings_operation &op) {
        document body;

        log_operation("transfer_from_savings");

        format_value(body, "from", op.from);
        format_value(body, "to", op.to);
        format_asset(op.amount, body, "amount");
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
        format_asset(op.amount_in, body, "amount_in");
        format_asset(op.amount_out, body, "amount_out");

        data << "fill_convert_request" << body;
    }

    void operation_writer::operator()(const author_reward_operation &op) {
        document body;

        log_operation("author_reward");

        format_value(body, "author", op.author);
        format_value(body, "permlink", op.permlink);
        format_asset(op.sbd_payout, body, "sbd_payout");
        format_asset(op.steem_payout, body, "steem_payout");
        format_asset(op.vesting_payout, body, "vesting_payout");

        data << "author_reward" << body;
    }

    void operation_writer::operator()(const curation_reward_operation &op) {
        document body;

        log_operation("curation_reward");

        format_value(body, "curator", op.curator);
        format_asset(op.reward, body, "reward");
        format_value(body, "comment_author", op.comment_author);
        format_value(body, "comment_permlink", op.comment_permlink);

        data << "curation_reward" << body;
    }

    void operation_writer::operator()(const comment_reward_operation &op) {
        document body;

        log_operation("comment_reward");

        format_value(body, "author", op.author);
        format_value(body, "permlink", op.permlink);
        format_asset(op.payout, body, "payout");

        data << "comment_reward" << body;
    }

    void operation_writer::operator()(const liquidity_reward_operation &op) {
        document body;

        log_operation("liquidity_reward");

        format_value(body, "owner", op.owner);
        format_asset(op.payout, body, "payout");

        data << "liquidity_reward" << body;
    }

    void operation_writer::operator()(const interest_operation &op) {
        document body;

        log_operation("interest");

        format_value(body, "owner", op.owner);
        format_asset(op.interest, body, "interest");

        data << "interest" << body;
    }

    void operation_writer::operator()(const fill_vesting_withdraw_operation &op) {
        document body;

        log_operation("fill_vesting_withdraw");

        format_value(body, "from_account", op.from_account);
        format_value(body, "to_account", op.to_account);
        format_asset(op.withdrawn, body, "withdrawn");
        format_asset(op.deposited, body, "deposited");

        data << "fill_vesting_withdraw" << body;
    }

    void operation_writer::operator()(const fill_order_operation &op) {
        document body;

        log_operation("fill_order");

        format_value(body, "current_owner", op.current_owner);
        format_value(body, "current_orderid", std::to_string(op.current_orderid));
        format_asset(op.current_pays, body, "current_pays");
        format_value(body, "open_owner", op.open_owner);
        format_value(body, "open_orderid", std::to_string(op.open_orderid));
        format_asset(op.open_pays, body, "open_pays");

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
        format_asset(op.amount, body, "amount");
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

        format_comment(op.author, op.permlink, body);

        data << "comment_payout_update" << body;
    }

    void operation_writer::operator()(const comment_benefactor_reward_operation& op) {
        document body;

        log_operation("comment_benefactor_reward_operation");

        format_value(body, "benefactor", op.benefactor);
        format_value(body, "author", op.author);
        format_value(body, "permlink", op.permlink);
        format_asset(op.reward, body, "reward");

        data << "comment_benefactor_reward_operation" << body;
    }

    /////////////////////////////////////////////////

    std::string operation_name::get_result() const {
        return result;
    }

    void operation_name::operator()(const vote_operation &op) {
        result = "vote";
    }

    void operation_name::operator()(const comment_operation &op) {
        result = "comment";
    }

    void operation_name::operator()(const transfer_operation &op) {
        result = "transfer";
    }

    void operation_name::operator()(const transfer_to_vesting_operation &op) {
        result = "transfer_to_vesting";
    }

    void operation_name::operator()(const withdraw_vesting_operation &op) {
        result = "withdraw_vesting";
    }

    void operation_name::operator()(const limit_order_create_operation &op) {
        result = "limit_order_create";
    }

    void operation_name::operator()(const limit_order_cancel_operation &op) {
        result = "limit_order_cancel";
    }

    void operation_name::operator()(const feed_publish_operation &op) {
        result = "feed_publish";
    }

    void operation_name::operator()(const convert_operation &op) {
        result = "convert";
    }

    void operation_name::operator()(const account_create_operation &op) {
        result = "account_create";
    }

    void operation_name::operator()(const account_update_operation &op) {
        result = "account_update";
    }

    void operation_name::operator()(const witness_update_operation &op) {
        result = "witness_update";
    }

    void operation_name::operator()(const account_witness_vote_operation &op) {
        result = "account_witness_vote";
    }

    void operation_name::operator()(const account_witness_proxy_operation &op) {
        result = "account_witness_proxy";
    }

    void operation_name::operator()(const pow_operation &op) {
        result = "pow";
    }

    void operation_name::operator()(const custom_operation &op) {
        result = "custom";
    }

    void operation_name::operator()(const report_over_production_operation &op) {
        result = "report_over_production";
    }

    void operation_name::operator()(const delete_comment_operation &op) {
        result = "delete_comment";
    }

    void operation_name::operator()(const custom_json_operation &op) {
        result = "custom_json";
    }

    void operation_name::operator()(const comment_options_operation &op) {
        result = "comment_options";
    }

    void operation_name::operator()(const set_withdraw_vesting_route_operation &op) {
        result = "set_withdraw_vesting_route";
    }

    void operation_name::operator()(const limit_order_create2_operation &op) {
        result = "limit_order_create2";
    }

    void operation_name::operator()(const challenge_authority_operation &op) {
        result = "challenge_authority";
    }

    void operation_name::operator()(const prove_authority_operation &op) {
        result = "prove_authority";
    }

    void operation_name::operator()(const request_account_recovery_operation &op) {
        result = "request_account_recovery";
    }

    void operation_name::operator()(const recover_account_operation &op) {
        result = "recover_account";
    }

    void operation_name::operator()(const change_recovery_account_operation &op) {
        result = "change_recovery_account";
    }

    void operation_name::operator()(const escrow_transfer_operation &op) {
        result = "escrow_transfer";
    }

    void operation_name::operator()(const escrow_dispute_operation &op) {
        result = "escrow_dispute";
    }

    void operation_name::operator()(const escrow_release_operation &op) {
        result = "escrow_release";
    }

    void operation_name::operator()(const pow2_operation &op) {
        result = "pow2";
    }

    void operation_name::operator()(const escrow_approve_operation &op) {
        result = "escrow_approve";
    }

    void operation_name::operator()(const transfer_to_savings_operation &op) {
        result = "transfer_to_savings";
    }

    void operation_name::operator()(const transfer_from_savings_operation &op) {
        result = "transfer_from_savings";
    }

    void operation_name::operator()(const cancel_transfer_from_savings_operation &op) {
        result = "cancel_transfer_from_savings";
    }

    void operation_name::operator()(const custom_binary_operation &op) {
        result = "custom_binary";
    }

    void operation_name::operator()(const decline_voting_rights_operation &op) {
        result = "decline_voting_rights";
    }

    void operation_name::operator()(const reset_account_operation &op) {
        result = "reset_account";
    }

    void operation_name::operator()(const set_reset_account_operation &op) {
        result = "set_reset_account";
    }

    void operation_name::operator()(const fill_convert_request_operation &op) {
        result = "fill_convert_request";
    }

    void operation_name::operator()(const author_reward_operation &op) {
        result = "author_reward";
    }

    void operation_name::operator()(const curation_reward_operation &op) {
        result = "curation_reward";
    }

    void operation_name::operator()(const comment_reward_operation &op) {
        result = "comment_reward";
    }

    void operation_name::operator()(const liquidity_reward_operation &op) {
        result = "liquidity_reward";
    }

    void operation_name::operator()(const interest_operation &op) {
        result = "interest";
    }

    void operation_name::operator()(const fill_vesting_withdraw_operation &op) {
        result = "fill_vesting_withdraw";
    }

    void operation_name::operator()(const fill_order_operation &op) {
        result = "fill_order";
    }

    void operation_name::operator()(const shutdown_witness_operation &op) {
        result = "shutdown_witness";
    }

    void operation_name::operator()(const fill_transfer_from_savings_operation &op) {
        result = "fill_transfer_from_savings";
    }

    void operation_name::operator()(const hardfork_operation &op) {
        result = "hardfork";
    }

    void operation_name::operator()(const comment_payout_update_operation &op) {
        result = "comment_payout_update";
    }

    void operation_name::operator()(const comment_benefactor_reward_operation& op) {
        result = "comment_benefactor_reward_operation";
    }

}}}
