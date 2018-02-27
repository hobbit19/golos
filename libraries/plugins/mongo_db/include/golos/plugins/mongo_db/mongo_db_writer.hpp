#pragma once

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

using bsoncxx::builder::stream::close_array;
using bsoncxx::builder::stream::close_document;
using bsoncxx::builder::stream::document;
using bsoncxx::builder::stream::finalize;
using bsoncxx::builder::stream::open_array;
using bsoncxx::builder::stream::open_document;

#include <golos/protocol/block.hpp>

namespace golos {
namespace plugins {
namespace mongo_db {

    using golos::protocol::signed_block;

    class mongo_db_writer final {
    public:
        mongo_db_writer() = default;
        ~mongo_db_writer() = default;

        void initialize(const std::string& uri_str);

        void on_block(const signed_block& block);

    private:

        size_t processed_blocks = 0;

        std::string db_name;
        static const std::string blocks_col;
        static const std::string trans_col;

        mongocxx::instance mongo_inst;
        mongocxx::uri uri;
        mongocxx::client mongo_conn;
    };
}}}

