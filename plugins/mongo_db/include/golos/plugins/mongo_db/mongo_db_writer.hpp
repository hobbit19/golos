#pragma once
#include <golos/protocol/block.hpp>
#include <golos/chain/database.hpp>
#include <golos/protocol/transaction.hpp>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/stream/array.hpp>
#include <bsoncxx/builder/stream/value_context.hpp>

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>

#include <map>


namespace golos {
namespace plugins {
namespace mongo_db {

    using golos::protocol::signed_block;
    using golos::protocol::signed_transaction;
    using bsoncxx::builder::stream::document;
    using bsoncxx::builder::stream::open_document;

    class mongo_db_writer final {
    public:
        mongo_db_writer();
        ~mongo_db_writer() = default;

        bool initialize(const std::string& uri_str);

        void on_block(const signed_block& block);

    private:

        void write_blocks();
        void write_block(const signed_block& block, mongocxx::bulk_write& _bulk);
        document write_transaction(const signed_transaction& tran);

        uint64_t processed_blocks = 0;

        std::string db_name;
        static const std::string blocks;
        static const std::string transactions;
        static const std::string operations;

        // Key = Block num, Value = block
        uint32_t last_irreversible_block_num;
        std::map<uint32_t, signed_block> _blocks;

        // Mongo connection members
        mongocxx::instance mongo_inst;
        mongocxx::uri uri;
        mongocxx::client mongo_conn;
        mongocxx::collection blocks_table;
        mongocxx::options::bulk_write bulk_opts;

        golos::chain::database &_db;
    };
}}}

