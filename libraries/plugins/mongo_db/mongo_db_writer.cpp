#include <golos/plugins/mongo_db/mongo_db_writer.hpp>


namespace golos {
namespace plugins {
namespace mongo_db {

    using namespace bsoncxx::types;
    using namespace bsoncxx::builder;
    using bsoncxx::builder::basic::kvp;

    const std::string mongo_db_writer::blocks_col = "Blocks";

    mongo_db_writer::mongo_db_writer(const std::string& uri_str) {
        mongocxx::uri uri = mongocxx::uri {uri_str};

        mongo_conn = mongocxx::client {uri};

        db_name = uri.database();
    }

    void mongo_db_writer::on_block(const signed_block& block) {

        mongocxx::options::bulk_write bulk_opts;
        bulk_opts.ordered(false);
        mongocxx::bulk_write bulk_trans {bulk_opts};

        auto blocks = mongo_conn[db_name][blocks_col]; // Blocks

        stream::document block_doc{};
        const auto block_id = block.id();
        const auto block_id_str = block_id.str();
        const auto prev_block_id_str = block.previous.str();
        auto block_num = block.block_num();

        auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::microseconds{fc::time_point::now().time_since_epoch().count()});

        block_doc << "block_num" << std::to_string(block_num)
                  << "block_id" << block_id_str
                  << "prev_block_id" << prev_block_id_str
                  << "transaction_merkle_root" << block.transaction_merkle_root.str();


        ++processed_blocks;
    }

    mongo_db_writer::~mongo_db_writer() {
    }
}}}
