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

        array key_auths;
        for (auto it: op.owner.key_auths) {
            key_auths << (std::string)it.first;
            key_auths << std::to_string(it.second);
        }

        array acct_auths;
        for (auto it: op.owner.account_auths) {
            acct_auths << it.first;
            acct_auths << std::to_string(it.second);
        }

        document owner_doc;
        owner_doc << "account_auths" << acct_auths;
        owner_doc << "key_auths" << key_auths;
        owner_doc << "weight_threshold" << std::to_string(op.owner.weight_threshold);


        array posting_key_auths;
        for (auto it: op.posting.key_auths) {
            posting_key_auths << (std::string)it.first;
            posting_key_auths << std::to_string(it.second);
        }

        array posting_acct_auths;
        for (auto it: op.posting.account_auths) {
            posting_acct_auths << it.first;
            posting_acct_auths << std::to_string(it.second);
        }

        document posting_owner_doc;
        posting_owner_doc << "account_auths" << posting_acct_auths;
        posting_owner_doc << "key_auths" << posting_key_auths;
        posting_owner_doc << "weight_threshold" << std::to_string(op.owner.weight_threshold);


        body << "fee"  << op.fee.to_string()
             << "creator" << op.creator
             << "new_account_name" << op.new_account_name;
        body << "owner" << owner_doc;
        body << "json_metadata" << op.json_metadata
             << "memo_key" << (std::string)op.memo_key;
        body << "posting" << posting_owner_doc;


        retval << "account_create" << body;

        return retval;
    }
    document write_operation(const account_update_operation& op)  {
        document retval;
        document body;

        document owner_doc;
        if (op.owner) {
            array key_auths;
            for (auto it: op.owner->key_auths) {
                key_auths << (std::string)it.first;
                key_auths << std::to_string(it.second);
            }

            array acct_auths;
            for (auto it: op.owner->account_auths) {
                acct_auths << (std::string)it.first;
                acct_auths << std::to_string(it.second);
            }


            owner_doc << "account_auths" << acct_auths;
            owner_doc << "key_auths" << key_auths;
            owner_doc << "weight_threshold" << std::to_string(op.owner->weight_threshold);
        }


        document posting_owner_doc;
        if (op.posting) {

            array posting_key_auths;
            for (auto it: op.posting->key_auths) {
                posting_key_auths << (std::string)it.first;
                posting_key_auths << std::to_string(it.second);
            }

            array posting_acct_auths;
            for (auto it: op.posting->account_auths) {
                posting_acct_auths << (std::string)it.first;
                posting_acct_auths << std::to_string(it.second);
            }

            posting_owner_doc << "account_auths" <<  posting_acct_auths;
            posting_owner_doc << "key_auths" << posting_key_auths;
            posting_owner_doc << "weight_threshold" << std::to_string(op.owner->weight_threshold);
        }

        document active_owner_doc;
        if (op.active) {

            array active_key_auths;
            for (auto it: op.active->key_auths) {
                active_key_auths << (std::string)it.first;
                active_key_auths << std::to_string(it.second);
            }

            array active_acct_auths;
            for (auto it: op.active->account_auths) {
                active_acct_auths << (std::string)it.first;
                active_acct_auths << std::to_string(it.second);
            }

            active_owner_doc << "account_auths" << active_acct_auths;
            active_owner_doc << "key_auths" << active_key_auths;
            active_owner_doc << "weight_threshold" << std::to_string(op.active->weight_threshold);
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
    account_name_type owner;
    string url;
    public_key_type block_signing_key;
    chain_properties props;
    asset fee;

    document write_operation(const witness_update_operation& op) {
        document retval;
        document body;

        document props_doc;
        props_doc << "account_creation_fee" << op.props.account_creation_fee.to_string()
                  << "maximum_block_size" << std::to_string(op.props.maximum_block_size)
                  << "sbd_interest_rate" << std::to_string(op.props.sbd_interest_rate);

        body << "owner"  << op.owner
             << "fee" << op.fee.to_string()
             << "url" << op.url
             << "block_signing_key" << (std::string)op.block_signing_key;
        body << "props" << props_doc;

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

        document props_doc;
        props_doc << "account_creation_fee" << op.props.account_creation_fee.to_string()
                  << "maximum_block_size" << std::to_string(op.props.maximum_block_size)
                  << "sbd_interest_rate" << std::to_string(op.props.sbd_interest_rate);






        return retval;
    }
    document write_operation(const custom_operation& op);
    document write_operation(const report_over_production_operation& op);
    document write_operation(const delete_comment_operation& op);
    document write_operation(const custom_json_operation& op);
    document write_operation(const comment_options_operation& op);
    document write_operation(const set_withdraw_vesting_route_operation& op);
    document write_operation(const limit_order_create2_operation& op);
    document write_operation(const challenge_authority_operation& op);
    document write_operation(const prove_authority_operation& op);
    document write_operation(const request_account_recovery_operation& op);
    document write_operation(const recover_account_operation& op);
    document write_operation(const change_recovery_account_operation& op);
    document write_operation(const escrow_transfer_operation& op);
    document write_operation(const escrow_dispute_operation& op);
    document write_operation(const escrow_release_operation& op);
    document write_operation(const pow2_operation& op);
    document write_operation(const escrow_approve_operation& op);
    document write_operation(const transfer_to_savings_operation& op);
    document write_operation(const transfer_from_savings_operation& op);
    document write_operation(const cancel_transfer_from_savings_operation& op);
    document write_operation(const custom_binary_operation& op);
    document write_operation(const decline_voting_rights_operation& op);
    document write_operation(const reset_account_operation& op);
    document write_operation(const set_reset_account_operation& op);
    document write_operation(const fill_convert_request_operation& op);
    document write_operation(const author_reward_operation& op);
    document write_operation(const curation_reward_operation& op);
    document write_operation(const comment_reward_operation& op);
    document write_operation(const liquidity_reward_operation& op);
    document write_operation(const interest_operation& op);
    document write_operation(const fill_vesting_withdraw_operation& op);
    document write_operation(const fill_order_operation& op);
    document write_operation(const shutdown_witness_operation& op);
    document write_operation(const fill_transfer_from_savings_operation& op);
    document write_operation(const hardfork_operation& op);
    document write_operation(const comment_payout_update_operation& op);

}}}

