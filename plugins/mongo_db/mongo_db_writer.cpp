#include <golos/plugins/mongo_db/mongo_db_writer.hpp>
#include <golos/plugins/mongo_db/mongo_db_operations.hpp>
#include <golos/plugins/chain/plugin.hpp>
#include <golos/protocol/operations.hpp>

#include <fc/log/logger.hpp>
#include <appbase/application.hpp>

#include <mongocxx/exception/exception.hpp>
#include <bsoncxx/array/element.hpp>
#include <bsoncxx/builder/stream/array.hpp>


namespace golos {
namespace plugins {
namespace mongo_db {

    using namespace bsoncxx::types;
    using namespace bsoncxx::builder;
    using bsoncxx::builder::stream::array;
    using bsoncxx::builder::basic::kvp;

    const std::string mongo_db_writer::blocks = "Blocks";
    const std::string mongo_db_writer::transactions = "Transactions";
    const std::string mongo_db_writer::operations = "Operations";

    mongo_db_writer::mongo_db_writer() :
            _db(appbase::app().get_plugin<golos::plugins::chain::plugin>().db()) {
    }

    void mongo_db_writer::initialize(const std::string& uri_str) {

        try {
            uri = mongocxx::uri {uri_str};
            mongo_conn = mongocxx::client {uri};
            db_name = uri.database().empty() ? "Golos" : uri.database();
            blocks_table = mongo_conn[db_name][blocks]; // Blocks
            bulk_opts.ordered(false);
            ilog("MongoDB plugin initialized.");
        }
        catch (mongocxx::exception & ex) {
            ilog("Exception in MongoDB initialize: ${p}", ("p", ex.what()));
        }
    }

    void mongo_db_writer::on_block(const signed_block& block) {
        try {
            ilog("MongoDB on_block: pushing block number ${p}", ("p", block.block_num()));
            _blocks[block.block_num()] = block;

            // Update last irreversible block number
            last_irreversible_block_num = _db.last_non_undoable_block_num();
            if (last_irreversible_block_num >= _blocks.begin()->first) {
                // Having irreversible blocks. Writing em into Mongo.
                write_blocks();
            }

            ++processed_blocks;
        }
        catch (mongocxx::exception & ex) {
            ilog("Exception in MongoDB on_block: ${p}", ("p", ex.what()));
        }
        catch (...) {
            ilog("Unknown exception in MongoDB on_block");
        }
    }

    void mongo_db_writer::write_blocks() {
        try {
            if (_blocks.empty()) {
                return;
            }
            ilog("mongo_db_writer::write_blocks start");
            mongocxx::bulk_write _bulk{bulk_opts};

            // Write all the blocks that has num less then last irreversible block
            while (!_blocks.empty() && _blocks.begin()->first <= last_irreversible_block_num) {
                auto head_iter = _blocks.begin();
                write_block(head_iter->second, _bulk);
                _blocks.erase(head_iter);
            }

            if (!blocks_table.bulk_write(_bulk)) {
                ilog("Failed to write blocks to Mongo DB");
            }
        }
        catch (mongocxx::exception & ex) {
            ilog("Exception in MongoDB write_blocks: ${p}", ("p", ex.what()));
        }
        catch (...) {
            ilog("Unknown exception in MongoDB write_blocks");
        }
    }

    void mongo_db_writer::write_block(const signed_block& block, mongocxx::bulk_write& _bulk) {

        auto doc = document {};
        // First write some general information from Block
        doc << "block_num"      << std::to_string(block.block_num())
            << "block_id"       << block.id().str()
            << "prev_block_id"  << block.previous.str()
            << "timestamp"      << block.timestamp
            << "witness"        << block.witness
            << "created_at"     << fc::time_point::now();

        array tran_arr;
        if (!block.transactions.empty()) {
            // Now write every transaction from Block
            for (const auto& trx : block.transactions) {
                tran_arr << write_transaction(trx);
            }
            doc << transactions << tran_arr;
        }

        mongocxx::model::insert_one insert_msg{doc.view()};
        _bulk.append(insert_msg);
    }

    document mongo_db_writer::write_transaction(const signed_transaction& tran) {

        // Write transaction general information
        document doc;
        doc << "id"             << tran.id().str()
            << "ref_block_num"  << std::to_string(tran.ref_block_num)
            << "expiration"     << fc::time_point::now();

        array oper_arr;
        if (!tran.operations.empty()) {
            // Write every operation in transaction
            for (const auto& op : tran.operations) {

                operation_writer op_writer;
                op.visit(op_writer);

                oper_arr << op_writer.get_document();
            }
            doc << operations << oper_arr;
        }

        return doc;
    }
}}}
