#include <golos/plugins/mongo_db/mongo_db_plugin.hpp>
#include <golos/plugins/json_rpc/plugin.hpp>
#include <golos/plugins/chain/plugin.hpp>
#include <golos/protocol/block.hpp>

#include <golos/plugins/mongo_db/mongo_db_writer.hpp>

namespace golos {
namespace plugins {
namespace mongo_db {

    using golos::protocol::signed_block;

    class mongo_db_plugin::mongo_db_plugin_impl {
    public:
        mongo_db_plugin_impl(mongo_db_plugin &plugin)
                : _my(plugin),
                  _db(appbase::app().get_plugin<golos::plugins::chain::plugin>().db()) {
        }

        bool initialize(const std::string& uri) {
            return writer.initialize(uri);
        }

        ~mongo_db_plugin_impl() = default;

        void on_block(const signed_block &block) {
            writer.on_block(block);
        }

        golos::chain::database &database() const {
            return _db;
        }

        void shutdown() {
            writer.shutdown();
        }

        mongo_db_writer writer;
        mongo_db_plugin &_my;

        golos::chain::database &_db;
    };

    // Plugin
    mongo_db_plugin::mongo_db_plugin() {
    }

    mongo_db_plugin::~mongo_db_plugin() {
    }

    void mongo_db_plugin::set_program_options(
            boost::program_options::options_description &cli,
            boost::program_options::options_description &cfg) {
        cli.add_options()
                ("mongodb-uri",
                 boost::program_options::value<string>()->default_value("mongodb://172.17.0.1:27017/Golos"),
                "Mongo DB connection string");
        cfg.add(cli);
    }

    void mongo_db_plugin::plugin_initialize(const boost::program_options::variables_map &options) {
        try {
            ilog("mongo_db plugin: plugin_initialize() begin");

            // First init mongo db
            if (options.count("mongodb-uri")) {
                std::string uri_str = options.at("mongodb-uri").as<std::string>();
                ilog("Connecting MongoDB to ${u}", ("u", uri_str));

                _my.reset(new mongo_db_plugin_impl(*this));

                if (!_my->initialize(uri_str)) {
                    ilog("Cannot initialize MongoDB plugin. Plugin disabled.");
                    _my.reset();
                    return;
                }
                // Set applied block listener
                auto &db = _my->database();

                db.applied_block.connect([&](const signed_block &b) {
                    _my->on_block(b);
                });

            } else {
                ilog("Mongo plugin configured, but no mongodb-uri specified. Plugin disabled.");
            }

            ilog("mongo_db plugin: plugin_initialize() end");
        } FC_CAPTURE_AND_RETHROW()
    }

    void mongo_db_plugin::plugin_startup() {
        ilog("mongo_db plugin: plugin_startup() begin");

        ilog("mongo_db plugin: plugin_startup() end");
    }

    void mongo_db_plugin::plugin_shutdown() {
        ilog("mongo_db plugin: plugin_shutdown() begin");

        _my->shutdown();
        ilog("mongo_db plugin: plugin_shutdown() end");
    }

 }}} // namespace golos::plugins::mongo_db
