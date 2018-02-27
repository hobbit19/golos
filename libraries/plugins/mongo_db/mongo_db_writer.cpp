#include <golos/plugins/mongo_db/mongo_db_writer.hpp>
#include <fc/log/logger.hpp>
#include <appbase/application.hpp>
#include <golos/plugins/chain/plugin.hpp>

#include <mongocxx/exception/exception.hpp>


namespace golos {
namespace plugins {
namespace mongo_db {

    using namespace bsoncxx::types;
    using namespace bsoncxx::builder;
    using bsoncxx::builder::basic::kvp;

    const std::string mongo_db_writer::blocks_table = "Blocks";
    const std::string mongo_db_writer::trans_table = "Transactions";

    mongo_db_writer::mongo_db_writer() :
            _db(appbase::app().get_plugin<golos::plugins::chain::plugin>().db()) {
    }

    void mongo_db_writer::initialize(const std::string& uri_str) {

        try {
            uri = mongocxx::uri {uri_str};
            mongo_conn = mongocxx::client {uri};
            db_name = uri.database().empty() ? "Golos" : uri.database();
            ilog("MongoDB plugin initialized.");
        }
        catch (mongocxx::exception & ex) {
            ilog("Exception in MongoDB initialize: ${p}", ("p", ex.what()));
        }
    }

    void mongo_db_writer::on_block(const signed_block& block) {
        try {

            _blocks[block.block_num()] = block;

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
    }

    void mongo_db_writer::write_blocks() {

        mongocxx::options::bulk_write bulk_opts;
        bulk_opts.ordered(false);
        mongocxx::bulk_write _bulk{bulk_opts};

        // Write all the blocks that has num less then last irreversible block
        while (!_blocks.empty() && _blocks.begin()->first <= last_irreversible_block_num) {
            auto head_iter = _blocks.begin();
            write_block(head_iter->second, _bulk);
            _blocks.erase(head_iter);
        }

        auto blocks = mongo_conn[db_name][blocks_table]; // Blocks

        if (!blocks.bulk_write(_bulk)) {
            ilog("Failed to write blocks to Mongo DB");
        }
    }

    void mongo_db_writer::write_block(const signed_block& block, mongocxx::bulk_write& _bulk) {

        auto doc = document {};
        doc << "block_num"      << std::to_string(block.block_num())
            << "block_id"       << block.id().str()
            << "prev_block_id"  << block.previous.str()
            << "timestamp"      << block.timestamp
            << "witness"        << block.witness
            << "created_at"     << fc::time_point::now();

        if (!block.transactions.empty()) {
            auto in_array = doc << "Transactions" << open_array;

            int trx_num = -1;
            for (const auto& trx : block.transactions) {
                ++trx_num;

                in_array << open_document
                         << "id"            << trx.id().str()
                         << "sequence_num"  << std::to_string(trx_num)
                         << "ref_block_num" << std::to_string(trx.ref_block_num)
                         << "expiration"    << fc::time_point::now()
                         << close_document;
            }
            in_array << close_array;
        }

        mongocxx::model::insert_one insert_msg{doc.view()};
        _bulk.append(insert_msg);
    }
}}}
