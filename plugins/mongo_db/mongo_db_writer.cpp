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

    bool mongo_db_writer::initialize(const std::string& uri_str) {
        try {
            uri = mongocxx::uri {uri_str};
            mongo_conn = mongocxx::client {uri};
            db_name = uri.database().empty() ? "Golos" : uri.database();
            mongo_database = mongo_conn[db_name];
            init_table_names();

            bulk_opts.ordered(false);
            ilog("MongoDB plugin initialized.");

            return true;
        }
        catch (mongocxx::exception & ex) {
            ilog("Exception in MongoDB initialize: ${p}", ("p", ex.what()));
            return false;
        }
        catch (...) {
            ilog("Unknown exception in MongoDB");
            return false;
        }
    }

    void mongo_db_writer::init_table_names() {

        bool has_table = true;

        while (has_table) {
            std::string table_name = format_table_name(tables_count);

            // If such collection exists proceed to another one
            has_table = mongo_database.has_collection(table_name);

            if (has_table == false && tables_count > 1) {
                // In this case use the last found collection
                break;
            }

            std::shared_ptr<mongocxx::collection> table_ptr(new mongocxx::collection(mongo_database[table_name]));
            blocks_tables.push_back(table_ptr);

            ++tables_count;
        }
        if (!blocks_tables.empty()) {
            active_blocks_table = *blocks_tables.back();
        }
    }

    std::string mongo_db_writer::format_table_name(const size_t num) {
        return std::string("Blocks_") + std::to_string(num);
    }

    void mongo_db_writer::on_block(const signed_block& block) {
        try {
            _blocks[block.block_num()] = block;

            // Update last irreversible block number
            last_irreversible_block_num = _db.last_non_undoable_block_num();
            if (last_irreversible_block_num >= _blocks.begin()->first) {
                // Having irreversible blocks. Writing em into Mongo.
                write_blocks();
            }

            ++processed_blocks;
        }
        catch (fc::exception & ex) {
            ilog("fc::exception in MongoDB on_block: ${p}", ("p", ex.what()));
        }
        catch (mongocxx::exception & ex) {
            ilog("Exception in MongoDB on_block: ${p}", ("p", ex.what()));
        }
        catch (...) {
            ilog("Unknown exception in MongoDB");
        }
    }

    void mongo_db_writer::update_active_table() {
        if (current_table_size >= max_table_size) {
            current_table_size = 0;

            std::string table_name = format_table_name(tables_count);
            tables_count++;
            std::shared_ptr<mongocxx::collection> table_ptr(new mongocxx::collection(mongo_database[table_name]));
            blocks_tables.push_back(table_ptr);

            active_blocks_table = *table_ptr;
        }
    }

    void mongo_db_writer::write_blocks() {
        if (_blocks.empty()) {
            return;
        }
        mongocxx::bulk_write _bulk{bulk_opts};

        const int blocks_to_write = _blocks.size();
        // Write all the blocks that has num less then last irreversible block
        while (!_blocks.empty() && _blocks.begin()->first <= last_irreversible_block_num) {
            auto head_iter = _blocks.begin();
            write_block(head_iter->second, _bulk);
            _blocks.erase(head_iter);
        }

        update_active_table();

        if (!active_blocks_table.bulk_write(_bulk)) {
            ilog("Failed to write blocks to Mongo DB");
        }
        else {
            current_table_size += blocks_to_write;
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
                tran_arr << format_transaction(trx);
            }
            doc << transactions << tran_arr;
        }

        mongocxx::model::insert_one insert_msg{doc.view()};
        _bulk.append(insert_msg);
    }

    document mongo_db_writer::format_transaction(const signed_transaction& tran) {
        // Write transaction general information
        document doc;
        doc << "id"             << tran.id().str()
            << "ref_block_num"  << std::to_string(tran.ref_block_num)
            << "expiration"     << tran.expiration;

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
