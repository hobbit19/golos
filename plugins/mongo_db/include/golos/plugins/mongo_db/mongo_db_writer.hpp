#pragma once
#include <golos/protocol/block.hpp>
#include <golos/chain/database.hpp>
#include <golos/protocol/transaction.hpp>
#include <golos/protocol/operations.hpp>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/stream/array.hpp>
#include <bsoncxx/builder/stream/value_context.hpp>

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>


#include <thread>
#include <map>
#include <mutex>
#include <condition_variable>
#include <thirdparty/appbase/include/appbase/application.hpp>
#include <boost/asio/io_service.hpp>


namespace golos {
namespace plugins {
namespace mongo_db {

    using golos::protocol::signed_block;
    using golos::protocol::signed_transaction;
    using bsoncxx::builder::stream::document;
    using bsoncxx::builder::stream::open_document;
    using namespace golos::protocol;

    using bulk_ptr = std::shared_ptr<mongocxx::bulk_write>;

    class mongo_db_writer final {
    public:
        mongo_db_writer();
        ~mongo_db_writer();

        bool initialize(const std::string& uri_str, const bool write_raw, const std::vector<std::string>& op);

        void on_block(const signed_block& block);

    private:

        void write_blocks();
        void write_raw_block(const signed_block& block);
        void write_block_operations(const signed_block& block);

        void format_block_info(const signed_block& block, document& doc);
        void format_transaction_info(const signed_transaction& tran, document& doc);

        void write_data();


        mongocxx::collection get_active_collection(const std::string& collection_name);


        uint64_t processed_blocks = 0;
        uint64_t max_collection_size = 1000000;


        static const std::string blocks;
        static const std::string transactions;
        static const std::string operations;

        std::string db_name;

        // Key = Block num, Value = block
        uint32_t last_irreversible_block_num;
        std::map<uint32_t, signed_block> _blocks_buffer;
        std::map<uint32_t, signed_block> _blocks;
        // Table name, bulk write
        std::map<std::string, bulk_ptr> _formatted_blocks;
        std::map<std::string, mongocxx::collection> active_collections;

        bool write_raw_blocks;
        std::vector<std::string> write_operations;

        // Mongo connection members
        mongocxx::instance mongo_inst;
        mongocxx::database mongo_database;
        mongocxx::uri uri;
        mongocxx::client mongo_conn;
        mongocxx::options::bulk_write bulk_opts;

        golos::chain::database &_db;
    };
}}}

