#pragma once
#include <golos/protocol/block.hpp>
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

#include <map>


using bsoncxx::builder::stream::close_array;
using bsoncxx::builder::stream::close_document;
using bsoncxx::builder::stream::document;
using bsoncxx::builder::stream::finalize;
using bsoncxx::builder::stream::open_array;
using bsoncxx::builder::stream::open_document;


namespace golos {
namespace plugins {
namespace mongo_db {

    using golos::protocol::signed_block;

    class mongo_db_writer final {
    public:
        mongo_db_writer();
        ~mongo_db_writer() = default;

        void initialize(const std::string& uri_str);

        void on_block(const signed_block& block);

    private:

        void write_blocks();
        void write_block(const signed_block& block, mongocxx::bulk_write& _bulk);

        size_t processed_blocks = 0;

        std::string db_name;
        static const std::string blocks_table;
        static const std::string trans_table;

        // Key = Block num, Value = block
        uint32_t last_irreversible_block_num;
        std::map<uint32_t, signed_block> _blocks;

        // Mongo connection members
        mongocxx::instance mongo_inst;
        mongocxx::uri uri;
        mongocxx::client mongo_conn;

        golos::chain::database &_db;
    };
}}}

