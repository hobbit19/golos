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


#include <thread>
#include <map>
#include <mutex>
#include <condition_variable>


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
        ~mongo_db_writer();

        bool initialize(const std::string& uri_str);

        void on_block(const signed_block& block);

    private:

        void worker_thread_entrypoint();
        void write_blocks();
        void format_block(const signed_block& block);
        void format_transaction(const signed_transaction& tran, document& doc);
        void write_data();

        uint64_t processed_blocks = 0;
        uint64_t tables_count = 1;
        uint64_t max_table_size = 1000000;
        uint64_t current_table_size = 0;


        std::string db_name;
        static const std::string blocks;
        static const std::string transactions;
        static const std::string operations;

        // Key = Block num, Value = block
        uint32_t last_irreversible_block_num;
        std::map<uint32_t, signed_block> _blocks_buffer;
        std::map<uint32_t, signed_block> _blocks;
        std::map<std::string, std::shared_ptr<mongocxx::bulk_write> > _formatted_blocks;

        bool shut_down = false;
        std::mutex data_mutex;
        std::condition_variable data_cond;
        std::unique_ptr<std::thread> worker_thread;

        // Mongo connection members
        mongocxx::instance mongo_inst;
        mongocxx::database mongo_database;
        mongocxx::uri uri;
        mongocxx::client mongo_conn;
        mongocxx::options::bulk_write bulk_opts;

        golos::chain::database &_db;
    };
}}}

