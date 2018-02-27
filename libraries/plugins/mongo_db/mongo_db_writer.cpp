#include <golos/plugins/mongo_db/mongo_db_writer.hpp>
#include <fc/log/logger.hpp>
#include <mongocxx/exception/exception.hpp>


namespace golos {
namespace plugins {
namespace mongo_db {

    using namespace bsoncxx::types;
    using namespace bsoncxx::builder;
    using bsoncxx::builder::basic::kvp;

    const std::string mongo_db_writer::blocks_col = "Blocks";
    const std::string mongo_db_writer::trans_col = "Transactions";

    void mongo_db_writer::initialize(const std::string& uri_str) {

        try {
            uri = mongocxx::uri {uri_str};
            mongo_conn = mongocxx::client {uri};
            db_name = uri.database().empty() ? "Golos" : uri.database();
            ilog("MongoDB plugin initialized.");
        }
        catch (mongocxx::exception & ex) {
            ilog("Exception in MongoDB ctor: ${p}", ("p", ex.what()));
        }
    }

    void mongo_db_writer::on_block(const signed_block& block) {
        try {
            auto blocks = mongo_conn[db_name][blocks_col]; // Blocks

            auto doc = document {};
            doc << "block_num"      << std::to_string(block.block_num())
                << "block_id"       << block.id().str()
                << "prev_block_id"  << block.previous.str()
                << "timestamp"      << block.timestamp
                << "witness"        << block.witness
                << "created_at"     << fc::time_point::now();

            if (!block.transactions.empty()) {
                auto in_array = doc << trans_col << open_array;

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

            if (!blocks.insert_one(doc.view())) {
                ilog("Failed to insert block ${bid}", ("bid", block.id()));
            }

            ++processed_blocks;
        }
        catch (mongocxx::exception & ex) {
            ilog("Exception in MongoDB on_block: ${p}", ("p", ex.what()));
        }
    }

}}}
