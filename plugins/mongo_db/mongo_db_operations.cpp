#include <golos/plugins/mongo_db/mongo_db_operations.hpp>
#include <golos/plugins/follow/follow_objects.hpp>
#include <golos/plugins/follow/plugin.hpp>
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
    using golos::chain::to_string;

    // Helper functions
    void format_asset(document& asset_doc, const std::string& name, const asset& value) {

        asset_doc << name + "_value" << value.to_real();
        asset_doc << name + "_symbol" << value.symbol_name();
    }

    void format_value(document& doc, const std::string& name, const std::string& value) {
        doc << name << value;
    }

    void format_value(document& doc, const std::string& name, const bool value) {
        doc << name << value;
    }

    void format_value(document& doc, const std::string& name, const double value) {
        doc << name << value;
    }

    void format_value(document& doc, const std::string& name, const fc::uint128_t& value) {
        doc << name << static_cast<int64_t>(value.lo);
    }

    template <typename T>
    void format_value(document& doc, const std::string& name, const fc::fixed_string<T>& value) {
        doc << name << static_cast<std::string>(value);
    }

    template <typename T>
    void format_value(document& doc, const std::string& name, const T& value) {
        doc << name << static_cast<int64_t>(value);
    }

    template <typename T>
    void format_value(document& doc, const std::string& name, const fc::safe<T>& value) {
        doc << name << static_cast<int64_t>(value.value);
    }

    document format_authority(const authority& auth) {
        array account_auths_arr;
        for (const auto& iter : auth.account_auths) {
            document temp;
            temp << "public_key_type" << (std::string)iter.first
                 << "weight_type" << iter.second;
            account_auths_arr << temp;
        }
        array key_auths_arr;
        for (auto& iter : auth.key_auths) {
            document temp;
            temp << "public_key_type" << (std::string)iter.first
                 << "weight_type" << iter.second;
            key_auths_arr << temp;
        }

        document authority_doc;
        authority_doc << "owner" << auth.owner;
        format_value(authority_doc, "weight_threshold", auth.weight_threshold);
        authority_doc << "account_auths" << account_auths_arr;
        authority_doc << "key_auths" << key_auths_arr;

        return authority_doc;
    }

    std::string to_string(const signature_type& signature) {
        std::string retVal;

        for (auto& iter : signature) {
            retVal += iter;
        }
        return retVal;
    }

    document format_chain_properties(const chain_properties& props) {
        document props_doc;
        format_asset(props_doc, "account_creation_fee", props.account_creation_fee);
        format_value(props_doc, "maximum_block_size", props.maximum_block_size);
        format_value(props_doc, "sbd_interest_rate", props.sbd_interest_rate);
        return props_doc;
    }

    /////////////////////////////////////////////////

    operation_writer::operation_writer() :
            _db(appbase::app().get_plugin<golos::plugins::chain::plugin>().db()) {
        data = std::make_shared<document>();
    }

    document_ptr operation_writer::get_document() {
        return data;
    }

    std::vector<document_ptr> operation_writer::get_documents() {
        if (!data_buffer.empty()) {
            return data_buffer;
        }
        data_buffer.push_back(data);
        return data_buffer;
    }

    void operation_writer::log_operation(const std::string& name) {
        // ilog("MongoDB operation: ${p}", ("p", name));
    }

    void operation_writer::format_comment(document& comment_doc, const std::string& auth, const std::string& perm) {

        //ilog("MongoDB operation format_comment: ${p} ${e}", ("p", auth)("e", perm));

        try {
            const comment_object& comment_obj = _db.get_comment(auth, perm);
            auto key = std::string(comment_obj.author).append("/").append(to_string(comment_obj.permlink));
            const auto hash = fc::sha256::hash(key);

            format_value(comment_doc, "author", comment_obj.author);
            format_value(comment_doc, "permlink", to_string(comment_obj.permlink));
            format_value(comment_doc, "abs_rshares", comment_obj.abs_rshares);
            format_value(comment_doc, "active", comment_obj.active.to_iso_string());

            format_value(comment_doc, "allow_curation_rewards", comment_obj.allow_curation_rewards);
            format_value(comment_doc, "allow_replies", comment_obj.allow_replies);
            format_value(comment_doc, "allow_votes", comment_obj.allow_votes);
            format_value(comment_doc, "author_rewards", comment_obj.author_rewards);
            format_value(comment_doc, "body", to_string(comment_obj.body));
            format_value(comment_doc, "body_length", comment_obj.body.length());
            format_value(comment_doc, "cashout_time", comment_obj.cashout_time.to_iso_string());
            format_value(comment_doc, "category", to_string(comment_obj.category));
            format_value(comment_doc, "children", comment_obj.children);
            format_value(comment_doc, "children_abs_rshares", comment_obj.children_abs_rshares.value);
            format_value(comment_doc, "children_rshares2", comment_obj.children_rshares2);
            format_value(comment_doc, "created", comment_obj.created.to_iso_string());
            format_asset(comment_doc, "curator_payout_value", comment_obj.curator_payout_value);
            format_value(comment_doc, "depth", comment_obj.depth);
            format_value(comment_doc, "id", comment_obj.id._id);
            format_value(comment_doc, "_id", hash.str());
            format_value(comment_doc, "last_payout", comment_obj.last_payout.to_iso_string());
            format_value(comment_doc, "last_update", comment_obj.last_update.to_iso_string());
            format_asset(comment_doc, "max_accepted_payout", comment_obj.max_accepted_payout);
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
            format_value(comment_doc, "net_rshares", comment_obj.net_rshares.value);
            format_value(comment_doc, "net_votes", comment_obj.net_votes);
            format_value(comment_doc, "parent_author", comment_obj.parent_author);
            format_value(comment_doc, "parent_permlink", to_string(comment_obj.parent_permlink));
            // pending_payout_value
            format_value(comment_doc, "percent_steem_dollars", comment_obj.percent_steem_dollars);
            // replies
            format_value(comment_doc, "reward_weight", comment_obj.reward_weight);
            // root_title
            // scanned
            format_value(comment_doc, "title", to_string(comment_obj.title));
            format_asset(comment_doc, "total_payout_value", comment_obj.total_payout_value);
            // total_pending_payout_value
            format_value(comment_doc, "total_vote_weight", comment_obj.total_vote_weight);
            // url
            format_value(comment_doc, "vote_rshares", comment_obj.vote_rshares.value);
            // last_reply
            // last_reply_by
            format_value(comment_doc, "json_metadata", to_string(comment_obj.json_metadata));

            // format_comment_active_votes(comment_doc, comment_obj);

            // TODO: the following fields depends on follow-plugin operations - custom_json ...
            // format_value(comment_doc, "author_reputation", get_account_reputation(comment_obj.author));
            // format_reblogged_by(comment_obj, comment_doc);
        }
        catch (fc::exception& ex) {
            ilog("MongoDB operations fc::exception. ${e}", ("e", ex.what()));
        }
        catch (...) {
            ilog("Unknown exception during formatting comment.");
        }
    }

    void operation_writer::format_comment_active_votes(document& doc, const comment_object& comment) {

        array votes;
        const auto &idx = _db.get_index<comment_vote_index>().indices().get<by_comment_voter>();
        comment_object::id_type cid(comment.id);
        auto itr = idx.lower_bound(cid);

        for (; itr != idx.end() && itr->comment == cid; ++itr) {

            document vote_doc;

            const auto &vo = _db.get(itr->voter);

            format_value(vote_doc, "voter", vo.name);
            format_value(vote_doc, "weight", itr->weight);
            format_value(vote_doc, "rshares", itr->rshares);
            format_value(vote_doc, "percent", itr->vote_percent);
            format_value(vote_doc, "time", itr->last_update.to_iso_string());
            //format_value(vote_doc, "reputation", get_account_reputation(vo.name));

            votes << vote_doc;
        }

        doc << "active_votes" << votes;
    }

    void operation_writer::format_reblogged_by(document& doc, const comment_object& comm) {
        // depends on the follow-plugin
        if (!_db.has_index<blog_index>()) {
            return;
        }

        const auto& blog_idx = _db.get_index<blog_index, by_comment>();

        array result;

        auto itr = blog_idx.lower_bound(comm.id);

        int arr_size = 0;
        for (; itr != blog_idx.end() && itr->comment == comm.id && ++arr_size < 2000; ++itr) {
            result << itr->account;
        }

        doc << "reblogged_by" << result;
    }

    std::string operation_writer::get_account_reputation(const account_name_type& account) {
        // depends on the follow-plugin
        if (!_db.has_index<reputation_index>()) {
            return "0";
        }

        const auto& rep_idx = _db.get_index<reputation_index>().indices().get<by_account>();
        auto itr = rep_idx.find(account);

        if (rep_idx.end() != itr) {
            return itr->reputation;
        }

        return "0";
    }

    void operation_writer::operator()(const vote_operation &op) {
        document body;

        log_operation("vote");

        try {
            const comment_object& comment_obj = _db.get_comment(op.author, op.permlink);
            auto key = std::string(op.author).append("/").append(op.permlink).append("/").append(op.voter);
            const auto hash = fc::sha256::hash(key);

            format_value(body, "author", op.author);
            format_value(body, "permlink", op.permlink);
            format_value(body, "voter", op.voter);
            format_value(body, "comment", comment_obj.id._id);
            format_value(body, "_id", hash.str());
            format_value(body, "weight", op.weight);

            *data << "vote" << body;
        }
        catch (fc::exception& ex) {
            ilog("MongoDB operations fc::exception. ${e}", ("e", ex.what()));
        }
        catch (...) {
            ilog("Unknown exception during formatting comment.");
        }
    }

    void operation_writer::operator()(const comment_operation &op) {

        auto data_comm_obj = std::make_shared<document>();
        document body_comm_obj;

        // First write comment_object
        log_operation("comment_object");
        format_comment(body_comm_obj, op.author, op.permlink);

        *data_comm_obj << "comment_object" << body_comm_obj;

        data_buffer.push_back(data_comm_obj);


        // Now write comment_operation
        log_operation("comment_operation");

        auto data_comm_oper = std::make_shared<document>();
        document body_comm_oper;

        format_value(body_comm_oper, "parent_author", op.parent_author);
        format_value(body_comm_oper, "parent_permlink", op.parent_permlink);
        format_value(body_comm_oper, "author", op.author);
        format_value(body_comm_oper, "permlink", op.permlink);
        format_value(body_comm_oper, "title", op.title);
        format_value(body_comm_oper, "body", op.body);
        format_value(body_comm_oper, "json_metadata", op.json_metadata);

        *data_comm_oper << "comment_operation" << body_comm_oper;

        data_buffer.push_back(data_comm_oper);
    }

    void operation_writer::operator()(const transfer_operation &op) {
        document body;

        log_operation("transfer");

        format_value(body, "from", op.from);
        format_value(body, "to", op.to);
        format_asset(body, "amount", op.amount);
        format_value(body, "memo", op.memo);

        *data << "transfer" << body;
    }

    void operation_writer::operator()(const transfer_to_vesting_operation &op) {
        document body;

        log_operation("transfer_to_vesting");

        format_value(body, "from", op.from);
        format_value(body, "to", op.to);
        format_asset(body, "amount", op.amount);

        *data << "transfer_to_vesting" << body;
    }

    void operation_writer::operator()(const withdraw_vesting_operation &op) {
        document body;

        log_operation("withdraw_vesting");

        format_value(body, "account", op.account);
        format_asset(body, "vesting_shares", op.vesting_shares);

        *data << "withdraw_vesting" << body;
    }

    void operation_writer::operator()(const limit_order_create_operation &op) {
        document body;

        log_operation("limit_order_create");

        format_value(body, "owner", op.owner);
        format_value(body, "orderid", op.orderid);
        format_asset(body, "amount_to_sell", op.amount_to_sell);
        format_asset(body, "min_to_receive", op.min_to_receive);
        format_value(body, "expiration", op.expiration.to_iso_string());

        *data << "limit_order_create" << body;
    }

    void operation_writer::operator()(const limit_order_cancel_operation &op) {
        document body;

        log_operation("limit_order_cancel");

        format_value(body, "owner", op.owner);
        format_value(body, "orderid", op.orderid);

        *data << "limit_order_cancel" << body;
    }

    void operation_writer::operator()(const feed_publish_operation &op) {
        document body;

        log_operation("feed_publish");

        format_value(body, "publisher", op.publisher);
        format_value(body, "exchange_rate", op.exchange_rate.to_real());

        *data << "feed_publish" << body;
    }

    void operation_writer::operator()(const convert_operation &op) {
        document body;

        log_operation("convert");

        format_value(body, "owner", op.owner);
        format_value(body, "requestid", op.requestid);
        format_asset(body, "amount", op.amount);

        *data << "convert" << body;
    }

    void operation_writer::operator()(const account_create_operation &op) {
        document body;

        // TODO: account collection

        log_operation("account_create");

        format_asset(body, "fee", op.fee);
        format_value(body, "creator", op.creator);
        format_value(body, "new_account_name", op.new_account_name);
        body << "owner" << format_authority(op.owner);
        format_value(body, "json_metadata", op.json_metadata);
        format_value(body, "memo_key", (std::string)op.memo_key);
        body << "posting" << format_authority(op.posting);

        *data << "account_create" << body;
    }

    void operation_writer::operator()(const account_update_operation &op) {
        document body;

        // TODO: account collection

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

        *data << "account_update" << body;
    }

    void operation_writer::operator()(const witness_update_operation &op) {

        //TODO: witness collection

        document body;

        log_operation("witness_update");

        format_value(body, "owner", op.owner);
        format_asset(body, "fee", op.fee);
        format_value(body, "url", op.url);
        format_value(body, "block_signing_key", (std::string)op.block_signing_key);
        body << "props" << format_chain_properties(op.props);

        *data << "witness_update" << body;
    }

    void operation_writer::operator()(const account_witness_vote_operation &op) {

        // TODO: witness collection
        document body;

        log_operation("account_witness_vote");

        format_value(body, "account", op.account);
        format_value(body, "witness", op.witness);
        format_value(body, "approve", (op.approve ? std::string("true") : std::string("false")));

        *data << "account_witness_vote" << body;
    }

    void operation_writer::operator()(const account_witness_proxy_operation &op) {
        // TODO: witness collection
        document body;

        log_operation("account_witness_proxy");

        format_value(body, "account", op.account);
        format_value(body, "proxy", op.proxy);

        *data << "account_witness_proxy" << body;
    }

    void operation_writer::operator()(const pow_operation &op) {
        document body;

        log_operation("pow");

        document pow_doc;
        format_value(pow_doc, "worker", (std::string)op.work.worker);
        format_value(pow_doc, "input", op.work.input.str());
        format_value(pow_doc, "signature", to_string(op.work.signature));
        format_value(pow_doc, "work", op.work.work.str());

        format_value(body, "block_id", op.block_id.str());
        format_value(body, "worker_account", op.worker_account);
        format_value(body, "nonce", op.nonce);

        body << "props" << format_chain_properties(op.props);
        body << "work" << pow_doc;

        *data << "pow" << body;
    }

    void operation_writer::operator()(const custom_operation &op) {
        document body;

        log_operation("custom");

        format_value(body, "id", op.id);
        if (!op.required_auths.empty()) {
            array auths;
            for (auto iter : op.required_auths) {
                auths << iter;
            }
            body << "required_auths" << auths;
        }

        *data << "custom" << body;
    }

    void operation_writer::operator()(const report_over_production_operation &op) {
        document body;

        log_operation("report_over_production");

        document doc1;
        format_value(doc1, "id", op.first_block.id().str());
        format_value(doc1, "timestamp", op.first_block.timestamp.to_iso_string());
        format_value(doc1, "witness", op.first_block.witness);

        document doc2;
        format_value(doc2, "id", op.second_block.id().str());
        format_value(doc2, "timestamp", op.first_block.timestamp.to_iso_string());
        format_value(doc2, "witness", op.second_block.witness);

        format_value(body, "reporter", op.reporter);

        body << "first_block" << doc1;
        body << "second_block" << doc2;

        *data << "report_over_production" << body;
    }

    void operation_writer::operator()(const delete_comment_operation &op) {
        document body;

        log_operation("delete_comment");

        // TODO: remove comment - can't call format_comment

        *data << "delete_comment" << body;
    }

    void operation_writer::operator()(const custom_json_operation &op) {
        document body;

        log_operation("custom_json");

        format_value(body, "id", op.id);
        format_value(body, "json", op.json);

        *data << "custom_json" << body;
    }

    void operation_writer::operator()(const comment_options_operation &op) {
        document body;

        log_operation("comment_options");

        format_comment(body, op.author, op.permlink);

        // *data << "comment_options" << body;
        *data << "comment_object" << body;
    }

    void operation_writer::operator()(const set_withdraw_vesting_route_operation &op) {
        document body;

        log_operation("set_withdraw_vesting_route");

        format_value(body, "from_account", op.from_account);
        format_value(body, "to_account", op.to_account);
        format_value(body, "percent", op.percent);
        format_value(body, "auto_vest", op.auto_vest);

        *data << "set_withdraw_vesting_route" << body;
    }

    void operation_writer::operator()(const limit_order_create2_operation &op) {
        document body;

        log_operation("limit_order_create2");

        format_value(body, "owner", op.owner);
        format_value(body, "orderid", op.orderid);
        format_asset(body, "amount_to_sell", op.amount_to_sell);
        format_value(body, "fill_or_kill", op.fill_or_kill);
        format_value(body, "exchange_rate", op.exchange_rate.to_real());
        format_value(body, "expiration", op.expiration.to_iso_string());

        *data << "limit_order_create2" << body;
    }

    void operation_writer::operator()(const challenge_authority_operation &op) {
        document body;

        log_operation("challenge_authority");

        format_value(body, "challenger", op.challenger);
        format_value(body, "challenged", op.challenged);
        format_value(body, "require_owner", op.require_owner);

        *data << "challenge_authority" << body;
    }

    void operation_writer::operator()(const prove_authority_operation &op) {
        document body;

        log_operation("prove_authority");

        format_value(body, "challenged", op.challenged);
        format_value(body, "require_owner", op.require_owner);

        *data << "prove_authority" << body;
    }

    void operation_writer::operator()(const request_account_recovery_operation &op) {
        document body;

        log_operation("request_account_recovery");

        format_value(body, "recovery_account", op.recovery_account);
        format_value(body, "account_to_recover", op.account_to_recover);
        body << "new_owner_authority" << format_authority(op.new_owner_authority);

        *data << "request_account_recovery" << body;
    }

    void operation_writer::operator()(const recover_account_operation &op) {
        document body;

        log_operation("recover_account");

        format_value(body, "account_to_recover", op.account_to_recover);
        body << "new_owner_authority" << format_authority(op.new_owner_authority);
        body << "recent_owner_authority" << format_authority(op.recent_owner_authority);

        *data << "recover_account" << body;
    }

    void operation_writer::operator()(const change_recovery_account_operation &op) {
        document body;

        log_operation("change_recovery_account");

        format_value(body, "account_to_recover", op.account_to_recover);
        format_value(body, "new_recovery_account", op.new_recovery_account);

        *data << "change_recovery_account" << body;
    }

    void operation_writer::operator()(const escrow_transfer_operation &op) {
        document body;

        log_operation("escrow_transfer");

        format_value(body, "from", op.from);
        format_value(body, "to", op.to);
        format_value(body, "agent", op.agent);
        format_value(body, "escrow_id", op.escrow_id);

        format_asset(body, "sbd_amount", op.sbd_amount);
        format_asset(body, "steem_amount ", op.steem_amount);
        format_asset(body, "fee", op.fee);
        format_value(body, "json_meta", op.json_meta);

        *data << "escrow_transfer" << body;
    }

    void operation_writer::operator()(const escrow_dispute_operation &op) {
        document body;

        log_operation("escrow_dispute");

        format_value(body, "from", op.from);
        format_value(body, "to", op.to);
        format_value(body, "agent", op.agent);
        format_value(body, "who", op.who);
        format_value(body, "escrow_id", op.escrow_id);

        *data << "escrow_dispute" << body;
    }

    void operation_writer::operator()(const escrow_release_operation &op) {
        document body;

        log_operation("escrow_release");

        format_value(body, "from", op.from);
        format_value(body, "to", op.to);
        format_value(body, "agent", op.agent);
        format_value(body, "who", op.who);
        format_value(body, "receiver", op.receiver);
        format_value(body, "escrow_id", op.escrow_id);

        format_asset(body, "sbd_amount", op.sbd_amount);
        format_asset(body, "steem_amount", op.steem_amount);

        *data << "escrow_release" << body;
    }

    void operation_writer::operator()(const pow2_operation &op) {
        document body;

        log_operation("pow2");

        body << "props" << format_chain_properties(op.props);
        if (op.new_owner_key) {
            format_value(body, "new_owner_key", (std::string)(*op.new_owner_key));
        }

        *data << "pow2" << body;
    }

    void operation_writer::operator()(const escrow_approve_operation &op) {
        document body;

        log_operation("escrow_approve");

        format_value(body, "from", op.from);
        format_value(body, "to", op.to);
        format_value(body, "agent", op.agent);
        format_value(body, "who", op.who);
        format_value(body, "escrow_id", op.escrow_id);
        format_value(body, "approve", op.approve);

        *data << "escrow_approve" << body;
    }

    void operation_writer::operator()(const transfer_to_savings_operation &op) {
        document body;

        log_operation("transfer_to_savings");

        format_value(body, "from", op.from);
        format_value(body, "to", op.to);
        format_asset(body, "amount", op.amount);
        format_value(body, "memo", op.memo);

        *data << "transfer_to_savings" << body;
    }

    void operation_writer::operator()(const transfer_from_savings_operation &op) {
        document body;

        log_operation("transfer_from_savings");

        format_value(body, "from", op.from);
        format_value(body, "to", op.to);
        format_asset(body, "amount", op.amount);
        format_value(body, "memo", op.memo);
        format_value(body, "request_id", op.request_id);

        *data << "transfer_from_savings" << body;
    }

    void operation_writer::operator()(const cancel_transfer_from_savings_operation &op) {
        document body;

        log_operation("cancel_transfer_from_savings");

        format_value(body, "from", op.from);
        format_value(body, "request_id", op.request_id);

        *data << "cancel_transfer_from_savings" << body;
    }

    void operation_writer::operator()(const custom_binary_operation &op) {
        document body;

        log_operation("custom_binary");

        array required_owner_auths_arr;
        for (auto& iter : op.required_owner_auths) {
            required_owner_auths_arr << iter;
        }

        array required_active_auths_arr;
        for (auto& iter : op.required_active_auths) {
            required_active_auths_arr << iter;
        }

        array required_posting_auths_arr;
        for (auto& iter : op.required_posting_auths) {
            required_posting_auths_arr << iter;
        }

        array auths;
        for (auto& iter : op.required_auths) {
            auths << format_authority(iter);
        }

        format_value(body, "id", op.id);
        body << "required_owner_auths" << required_owner_auths_arr;
        body << "required_active_auths" << required_active_auths_arr;
        body << "required_posting_auths" << required_posting_auths_arr;
        body << "required_auths" << auths;

        *data << "custom_binary" << body;
    }

    void operation_writer::operator()(const decline_voting_rights_operation &op) {
        document body;

        log_operation("decline_voting_rights");

        format_value(body, "account", op.account);
        format_value(body, "decline", op.decline);

        *data << "decline_voting_rights" << body;
    }

    void operation_writer::operator()(const reset_account_operation &op) {
        document body;

        log_operation("reset_account");

        format_value(body, "reset_account", op.reset_account);
        format_value(body, "account_to_reset", op.account_to_reset);
        body << "new_owner_authority" << format_authority(op.new_owner_authority);

        *data << "reset_account" << body;
    }

    void operation_writer::operator()(const set_reset_account_operation &op) {
        document body;

        log_operation("set_reset_account");

        format_value(body, "account", op.account);
        format_value(body, "current_reset_account", op.current_reset_account);
        format_value(body, "reset_account", op.reset_account);

        *data << "set_reset_account" << body;
    }

    void operation_writer::operator()(const fill_convert_request_operation &op) {
        document body;

        log_operation("fill_convert_request");

        format_value(body, "owner", op.owner);
        format_value(body, "requestid", op.requestid);
        format_asset(body, "amount_in", op.amount_in);
        format_asset(body, "amount_out", op.amount_out);

        *data << "fill_convert_request" << body;
    }

    void operation_writer::operator()(const author_reward_operation &op) {
        document body;

        // TODO: author rewards collection

        log_operation("author_reward");

        format_value(body, "author", op.author);
        format_value(body, "permlink", op.permlink);
        format_asset(body, "sbd_payout", op.sbd_payout);
        format_asset(body, "steem_payout", op.steem_payout);
        format_asset(body, "vesting_payout", op.vesting_payout);

        *data << "author_reward" << body;
    }

    void operation_writer::operator()(const curation_reward_operation &op) {
        document body;

        // TODO: curation rewards collection

        log_operation("curation_reward");

        format_value(body, "curator", op.curator);
        format_asset(body, "reward", op.reward);
        format_value(body, "comment_author", op.comment_author);
        format_value(body, "comment_permlink", op.comment_permlink);

        *data << "curation_reward" << body;
    }

    void operation_writer::operator()(const comment_reward_operation &op) {
        document body;

        // TODO: comment rewards collection

        log_operation("comment_reward");

        format_value(body, "author", op.author);
        format_value(body, "permlink", op.permlink);
        format_asset(body, "payout", op.payout);

        *data << "comment_reward" << body;
    }

    void operation_writer::operator()(const liquidity_reward_operation &op) {
        document body;

        log_operation("liquidity_reward");

        format_value(body, "owner", op.owner);
        format_asset(body, "payout", op.payout);

        *data << "liquidity_reward" << body;
    }

    void operation_writer::operator()(const interest_operation &op) {
        document body;

        log_operation("interest");

        format_value(body, "owner", op.owner);
        format_asset(body, "interest", op.interest);

        *data << "interest" << body;
    }

    void operation_writer::operator()(const fill_vesting_withdraw_operation &op) {
        document body;

        log_operation("fill_vesting_withdraw");

        format_value(body, "from_account", op.from_account);
        format_value(body, "to_account", op.to_account);
        format_asset(body, "withdrawn", op.withdrawn);
        format_asset(body, "deposited", op.deposited);

        *data << "fill_vesting_withdraw" << body;
    }

    void operation_writer::operator()(const fill_order_operation &op) {
        document body;

        log_operation("fill_order");

        format_value(body, "current_owner", op.current_owner);
        format_value(body, "current_orderid", op.current_orderid);
        format_asset(body, "current_pays", op.current_pays);
        format_value(body, "open_owner", op.open_owner);
        format_value(body, "open_orderid", op.open_orderid);
        format_asset(body, "open_pays", op.open_pays);

        *data << "fill_order" << body;
    }

    void operation_writer::operator()(const shutdown_witness_operation &op) {
        document body;

        log_operation("shutdown_witness");

        format_value(body, "owner", op.owner);

        *data << "shutdown_witness" << body;
    }

    void operation_writer::operator()(const fill_transfer_from_savings_operation &op) {
        document body;

        log_operation("fill_transfer_from_savings");

        format_value(body, "from", op.from);
        format_value(body, "to", op.to);
        format_asset(body, "amount", op.amount);
        format_value(body, "request_id", op.request_id);
        format_value(body, "memo", op.memo);

        *data << "fill_transfer_from_savings" << body;
    }

    void operation_writer::operator()(const hardfork_operation &op) {
        document body;

        log_operation("hardfork");

        format_value(body, "hardfork_id", op.hardfork_id);

        *data << "hardfork" << body;
    }

    void operation_writer::operator()(const comment_payout_update_operation &op) {
        document body;

        log_operation("comment_payout_update");

        // format_comment(body, op.author, op.permlink);

        // *data << "comment_payout_update" << body;
        *data << "comment_object" << body;
    }

    void operation_writer::operator()(const comment_benefactor_reward_operation& op) {
        document body;

        // TODO: beneficiar rewards collection

        log_operation("comment_benefactor_reward_operation");

        format_value(body, "benefactor", op.benefactor);
        format_value(body, "author", op.author);
        format_value(body, "permlink", op.permlink);
        format_asset(body, "reward", op.reward);

        *data << "comment_benefactor_reward_operation" << body;
    }

    /////////////////////////////////////////////////

    std::string operation_name::operator()(const vote_operation &op) const {
        return "vote";
    }

    std::string operation_name::operator()(const comment_operation &op) const {
        // This operation is parsed into 2 comment_object andcomment_operation
        // in operation_parser
        return "comment_operation";
    }

    std::string operation_name::operator()(const transfer_operation &op) const {
        return "transfer";
    }

    std::string operation_name::operator()(const transfer_to_vesting_operation &op) const {
        return "transfer_to_vesting";
    }

    std::string operation_name::operator()(const withdraw_vesting_operation &op) const {
        return "withdraw_vesting";
    }

    std::string operation_name::operator()(const limit_order_create_operation &op) const {
        return "limit_order_create";
    }

    std::string operation_name::operator()(const limit_order_cancel_operation &op) const {
        return "limit_order_cancel";
    }

    std::string operation_name::operator()(const feed_publish_operation &op) const {
        return "feed_publish";
    }

    std::string operation_name::operator()(const convert_operation &op) const {
        return "convert";
    }

    std::string operation_name::operator()(const account_create_operation &op) const {
        return "account_create";
    }

    std::string operation_name::operator()(const account_update_operation &op) const {
        return "account_update";
    }

    std::string operation_name::operator()(const witness_update_operation &op) const {
        return "witness_update";
    }

    std::string operation_name::operator()(const account_witness_vote_operation &op) const {
        return "account_witness_vote";
    }

    std::string operation_name::operator()(const account_witness_proxy_operation &op) const {
        return "account_witness_proxy";
    }

    std::string operation_name::operator()(const pow_operation &op) const {
        return "pow";
    }

    std::string operation_name::operator()(const custom_operation &op) const {
        return "custom";
    }

    std::string operation_name::operator()(const report_over_production_operation &op) const {
        return "report_over_production";
    }

    std::string operation_name::operator()(const delete_comment_operation &op) const {
        return "delete_comment";
    }

    std::string operation_name::operator()(const custom_json_operation &op) const {
        return "custom_json";
    }

    std::string operation_name::operator()(const comment_options_operation &op) const {
        return "comment_options";
    }

    std::string operation_name::operator()(const set_withdraw_vesting_route_operation &op) const {
        return "set_withdraw_vesting_route";
    }

    std::string operation_name::operator()(const limit_order_create2_operation &op) const {
        return "limit_order_create2";
    }

    std::string operation_name::operator()(const challenge_authority_operation &op) const {
        return "challenge_authority";
    }

    std::string operation_name::operator()(const prove_authority_operation &op) const {
        return "prove_authority";
    }

    std::string operation_name::operator()(const request_account_recovery_operation &op) const {
        return "request_account_recovery";
    }

    std::string operation_name::operator()(const recover_account_operation &op) const {
        return "recover_account";
    }

    std::string operation_name::operator()(const change_recovery_account_operation &op) const {
        return "change_recovery_account";
    }

    std::string operation_name::operator()(const escrow_transfer_operation &op) const {
        return "escrow_transfer";
    }

    std::string operation_name::operator()(const escrow_dispute_operation &op) const {
        return "escrow_dispute";
    }

    std::string operation_name::operator()(const escrow_release_operation &op) const {
        return "escrow_release";
    }

    std::string operation_name::operator()(const pow2_operation &op) const {
        return "pow2";
    }

    std::string operation_name::operator()(const escrow_approve_operation &op) const {
        return "escrow_approve";
    }

    std::string operation_name::operator()(const transfer_to_savings_operation &op) const {
        return "transfer_to_savings";
    }

    std::string operation_name::operator()(const transfer_from_savings_operation &op) const {
        return "transfer_from_savings";
    }

    std::string operation_name::operator()(const cancel_transfer_from_savings_operation &op) const {
        return "cancel_transfer_from_savings";
    }

    std::string operation_name::operator()(const custom_binary_operation &op) const {
        return "custom_binary";
    }

    std::string operation_name::operator()(const decline_voting_rights_operation &op) const {
        return "decline_voting_rights";
    }

    std::string operation_name::operator()(const reset_account_operation &op) const {
        return "reset_account";
    }

    std::string operation_name::operator()(const set_reset_account_operation &op) const {
        return "set_reset_account";
    }

    std::string operation_name::operator()(const fill_convert_request_operation &op) const {
        return "fill_convert_request";
    }

    std::string operation_name::operator()(const author_reward_operation &op) const {
        return "author_reward";
    }

    std::string operation_name::operator()(const curation_reward_operation &op) const {
        return "curation_reward";
    }

    std::string operation_name::operator()(const comment_reward_operation &op) const {
        return "comment_reward";
    }

    std::string operation_name::operator()(const liquidity_reward_operation &op) const {
        return "liquidity_reward";
    }

    std::string operation_name::operator()(const interest_operation &op) const {
        return "interest";
    }

    std::string operation_name::operator()(const fill_vesting_withdraw_operation &op) const {
        return "fill_vesting_withdraw";
    }

    std::string operation_name::operator()(const fill_order_operation &op) const {
        return "fill_order";
    }

    std::string operation_name::operator()(const shutdown_witness_operation &op) const {
        return "shutdown_witness";
    }

    std::string operation_name::operator()(const fill_transfer_from_savings_operation &op) const {
        return "fill_transfer_from_savings";
    }

    std::string operation_name::operator()(const hardfork_operation &op) const {
        return "hardfork";
    }

    std::string operation_name::operator()(const comment_payout_update_operation &op) const {
        return "comment_payout_update";
    }

    std::string operation_name::operator()(const comment_benefactor_reward_operation& op) const {
        return "comment_benefactor_reward_operation";
    }


    operation_parser::operation_parser(const operation& op) : oper(op) {

        operation_writer op_writer;
        op.visit(op_writer);

        // FIXME: what is happens here ????

        if (!op_writer.single_document()) {
            std::vector<document_ptr> docs = op_writer.get_documents();
            // Okay this looks very ugly but for now we got only 1 operations
            // that has 2 separate documents for different collections inside

            if (docs.size() == 2) {
                auto doc1 = std::make_unique<named_document>();
                doc1->doc = docs[0];
                doc1->collection_name = "comment_object";
                documents.push_back(std::move(doc1));

                auto doc2 = std::make_unique<named_document>();
                doc2->doc = docs[1];
                doc2->collection_name = "comment_operation";
                documents.push_back(std::move(doc2));
            }
        }
        else {
            auto doc1 = std::make_unique<named_document>();
            doc1->doc = op_writer.get_document();

            doc1->collection_name = op.visit(operation_name());

            documents.push_back(std::move(doc1));
        }
    }
}}}
