#pragma once
#include <golos/protocol/operations.hpp>
#include <golos/chain/database.hpp>

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

    using namespace golos::chain;
    using namespace golos::protocol;
    using bsoncxx::builder::stream::document;

    using document_ptr = std::shared_ptr<document>;

    struct named_document {
        named_document() = default;
        document_ptr doc;
        std::string collection_name;
    };

    using named_doc_ptr = std::shared_ptr<named_document>;

    class operation_writer {
    public:

        operation_writer();

        typedef void result_type;

        document_ptr get_document();
        std::vector<document_ptr> get_documents();
        bool single_document() const { return data_buffer.empty(); }

        void format_comment(const std::string& auth, const std::string& perm, document& comment_doc);
        void format_comment_active_votes(const comment_object& comm, document& doc);
        void format_reblogged_by(const comment_object& comm, document& doc);

        std::string get_account_reputation(const account_name_type& account);


        void operator()(const vote_operation &op);
        void operator()(const comment_operation &op);
        void operator()(const transfer_operation &op);
        void operator()(const transfer_to_vesting_operation &op);
        void operator()(const withdraw_vesting_operation &op);
        void operator()(const limit_order_create_operation &op);
        void operator()(const limit_order_cancel_operation &op);
        void operator()(const feed_publish_operation &op);
        void operator()(const convert_operation &op);
        void operator()(const account_create_operation &op);
        void operator()(const account_update_operation &op);
        void operator()(const witness_update_operation &op);
        void operator()(const account_witness_vote_operation &op);
        void operator()(const account_witness_proxy_operation &op);
        void operator()(const pow_operation &op);
        void operator()(const custom_operation &op);
        void operator()(const report_over_production_operation &op);
        void operator()(const delete_comment_operation &op);
        void operator()(const custom_json_operation &op);
        void operator()(const comment_options_operation &op);
        void operator()(const set_withdraw_vesting_route_operation &op);
        void operator()(const limit_order_create2_operation &op);
        void operator()(const challenge_authority_operation &op);
        void operator()(const prove_authority_operation &op);
        void operator()(const request_account_recovery_operation &op);
        void operator()(const recover_account_operation &op);
        void operator()(const change_recovery_account_operation &op);
        void operator()(const escrow_transfer_operation &op);
        void operator()(const escrow_dispute_operation &op);
        void operator()(const escrow_release_operation&op);
        void operator()(const pow2_operation &op);
        void operator()(const escrow_approve_operation &op);
        void operator()(const transfer_to_savings_operation &op);
        void operator()(const transfer_from_savings_operation &op);
        void operator()(const cancel_transfer_from_savings_operation&op);
        void operator()(const custom_binary_operation &op);
        void operator()(const decline_voting_rights_operation &op);
        void operator()(const reset_account_operation &op);
        void operator()(const set_reset_account_operation &op);
        void operator()(const fill_convert_request_operation &op);
        void operator()(const author_reward_operation &op);
        void operator()(const curation_reward_operation &op);
        void operator()(const comment_reward_operation &op);
        void operator()(const liquidity_reward_operation &op);
        void operator()(const interest_operation &op);
        void operator()(const fill_vesting_withdraw_operation &op);
        void operator()(const fill_order_operation &op);
        void operator()(const shutdown_witness_operation &op);
        void operator()(const fill_transfer_from_savings_operation &op);
        void operator()(const hardfork_operation &op);
        void operator()(const comment_payout_update_operation &op);
        void operator()(const comment_benefactor_reward_operation& op);

    private:
        void log_operation(const std::string& name);
        document_ptr data;
        // Additional buffer
        std::vector<document_ptr> data_buffer;
        database &_db;
    };

    class operation_name {
    public:

        typedef void result_type;

        void operator()(const vote_operation &op);
        void operator()(const comment_operation &op);
        void operator()(const transfer_operation &op);
        void operator()(const transfer_to_vesting_operation &op);
        void operator()(const withdraw_vesting_operation &op);
        void operator()(const limit_order_create_operation &op);
        void operator()(const limit_order_cancel_operation &op);
        void operator()(const feed_publish_operation &op);
        void operator()(const convert_operation &op);
        void operator()(const account_create_operation &op);
        void operator()(const account_update_operation &op);
        void operator()(const witness_update_operation &op);
        void operator()(const account_witness_vote_operation &op);
        void operator()(const account_witness_proxy_operation &op);
        void operator()(const pow_operation &op);
        void operator()(const custom_operation &op);
        void operator()(const report_over_production_operation &op);
        void operator()(const delete_comment_operation &op);
        void operator()(const custom_json_operation &op);
        void operator()(const comment_options_operation &op);
        void operator()(const set_withdraw_vesting_route_operation &op);
        void operator()(const limit_order_create2_operation &op);
        void operator()(const challenge_authority_operation &op);
        void operator()(const prove_authority_operation &op);
        void operator()(const request_account_recovery_operation &op);
        void operator()(const recover_account_operation &op);
        void operator()(const change_recovery_account_operation &op);
        void operator()(const escrow_transfer_operation &op);
        void operator()(const escrow_dispute_operation &op);
        void operator()(const escrow_release_operation&op);
        void operator()(const pow2_operation &op);
        void operator()(const escrow_approve_operation &op);
        void operator()(const transfer_to_savings_operation &op);
        void operator()(const transfer_from_savings_operation &op);
        void operator()(const cancel_transfer_from_savings_operation&op);
        void operator()(const custom_binary_operation &op);
        void operator()(const decline_voting_rights_operation &op);
        void operator()(const reset_account_operation &op);
        void operator()(const set_reset_account_operation &op);
        void operator()(const fill_convert_request_operation &op);
        void operator()(const author_reward_operation &op);
        void operator()(const curation_reward_operation &op);
        void operator()(const comment_reward_operation &op);
        void operator()(const liquidity_reward_operation &op);
        void operator()(const interest_operation &op);
        void operator()(const fill_vesting_withdraw_operation &op);
        void operator()(const fill_order_operation &op);
        void operator()(const shutdown_witness_operation &op);
        void operator()(const fill_transfer_from_savings_operation &op);
        void operator()(const hardfork_operation &op);
        void operator()(const comment_payout_update_operation &op);
        void operator()(const comment_benefactor_reward_operation& op);

        std::string get_result() const;
    private:
        std::string result;
    };

    class operation_parser {
    public:
        operation_parser(const operation& op);

        std::vector<named_doc_ptr> documents;
        const operation& oper;
    };
}}}
