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

    mongo_db_writer::mongo_db_writer() {
    }

    bool mongo_db_writer::initialize(const std::string& uri_str) {
        try {
            mongocxx::uri uri = mongocxx::uri {uri_str};

            mongo_conn = mongocxx::client {uri};
            db_name = uri.database().empty() ? "Golos" : uri.database();
        }
        catch (mongocxx::exception & ex) {
            ilog(ex.what());
            return false;
        }
        return true;
    }

    void mongo_db_writer::on_block(const signed_block& block) {
        try {
            auto blocks = mongo_conn[db_name][blocks_col]; // Blocks

            auto doc = document {};

            auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::microseconds{fc::time_point::now().time_since_epoch().count()});

            auto doc_ctx = doc
                    << "block_num" << std::to_string(block.block_num())
                    << "block_id" << block.id().str()
                    << "prev_block_id" << block.previous.str()
                    << "merkle_root" << block.transaction_merkle_root.str()
                    << "created_at" << std::to_string(now.count());

            if (!block.transactions.empty()) {
                auto in_array = doc << "transactions" << open_array;

                int trx_num = -1;
                for (const auto& trx : block.transactions) {
                    ++trx_num;

                    const auto trans_id_str = trx.id().str();
                    in_array << open_document
                             << "id" << trans_id_str
                             << "sequence_num" << std::to_string(trx_num)
                             << "ref_block_num" << std::to_string(trx.ref_block_num)
                             << "expiration" << std::to_string(now.count())
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
            ilog(ex.what());
            throw;
        }
    }

    mongo_db_writer::~mongo_db_writer() {
    }
}}}