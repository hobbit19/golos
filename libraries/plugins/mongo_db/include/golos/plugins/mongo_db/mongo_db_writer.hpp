#pragma once

#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/json.hpp>

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>

#include <golos/protocol/block.hpp>

namespace golos {
namespace plugins {
namespace mongo_db {

    using golos::protocol::signed_block;

    class mongo_db_writer final {
    public:
        mongo_db_writer(const std::string& uri_str);
        ~mongo_db_writer();

        void on_block(const signed_block& block);

    private:

        size_t processed_blocks = 0;

        std::string db_name;
        static const std::string blocks_col;

        mongocxx::instance mongo_inst;
        mongocxx::client mongo_conn;
    };
}}}

