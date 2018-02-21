#include <golos/plugins/mongo_db/mongo_db_plugin.hpp>
#include <golos/plugins/json_rpc/plugin.hpp>
#include <golos/plugins/chain/plugin.hpp>
#include <golos/protocol/block.hpp>


#define CHECK_ARG_SIZE(s) \
   FC_ASSERT( args.args->size() == s, "Expected #s argument(s), was ${n}", ("n", args.args->size()) );


namespace golos {
namespace plugins {
namespace mongo_db {

    using golos::plugins::json_rpc::void_type;

    class mongo_db_plugin::mongo_db_plugin_impl {
    public:
        mongo_db_plugin_impl(mongo_db_plugin &plugin)
                : _my(plugin),
                  _db(appbase::app().get_plugin<golos::plugins::chain::plugin>().db()){
        }

        ~mongo_db_plugin_impl() {
        }

        void process_block(const golos::protocol::signed_block &block);

        golos::chain::database &database() const {
            return _db;
        }

        mongo_db_plugin &_my;

        golos::chain::database &_db;
    };

    void mongo_db_plugin::mongo_db_plugin_impl::process_block(const golos::protocol::signed_block &block) {

    }

    // Plugin
    mongo_db_plugin::mongo_db_plugin() {
    }

    mongo_db_plugin::~mongo_db_plugin() {

    }

    void mongo_db_plugin::set_program_options(
            boost::program_options::options_description &cli,
            boost::program_options::options_description &cfg) {

    }

    void mongo_db_plugin::plugin_initialize(const boost::program_options::variables_map &options) {
        try {
            ilog("mongo_db plugin: plugin_initialize() begin");
            _my.reset(new mongo_db_plugin_impl(*this));

            ilog("mongo_db plugin: plugin_initialize() end");
            JSON_RPC_REGISTER_API ( name() ) ;
        } FC_CAPTURE_AND_RETHROW()
    }

    void mongo_db_plugin::plugin_startup() {
        ilog("mongo_db plugin: plugin_startup() begin");

        ilog("mongo_db plugin: plugin_startup() end");
    }

    void mongo_db_plugin::plugin_shutdown() {
        ilog("mongo_db plugin: plugin_shutdown() begin");

        ilog("mongo_db plugin: plugin_shutdown() end");
    }

    DEFINE_API(mongo_db_plugin, process_block) {
        CHECK_ARG_SIZE(1)
        auto block = args.args->at(0).as<golos::protocol::signed_block>();
        auto &db = _my->database();
        return db.with_read_lock([&]() {
            _my->process_block(block);
            return void_type();
        });
    }

 }}} // namespace golos::plugins::mongo_db
