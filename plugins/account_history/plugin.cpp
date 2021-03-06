#include <golos/plugins/account_history/plugin.hpp>

#include <golos/chain/operation_notification.hpp>
#include <golos/chain/history_object.hpp>

#include <fc/smart_ref_impl.hpp>

#include <boost/algorithm/string.hpp>
#define STEEM_NAMESPACE_PREFIX "golos::protocol::"

namespace golos {
namespace plugins {
namespace account_history {

struct operation_visitor_filter;
void operation_get_impacted_accounts(const operation &op, flat_set<golos::chain::account_name_type> &result);

using namespace golos::protocol;
using namespace golos::chain;
// 
template<typename T>
T dejsonify(const string &s) {
    return fc::json::from_string(s).as<T>();
}

#define DEFAULT_VALUE_VECTOR(value) default_value({fc::json::to_string(value)}, fc::json::to_string(value))
#define LOAD_VALUE_SET(options, name, container, type) \
if( options.count(name) ) { \
    const std::vector<std::string>& ops = options[name].as<std::vector<std::string>>(); \
    std::transform(ops.begin(), ops.end(), std::inserter(container, container.end()), &dejsonify<type>); \
}
// 

struct operation_visitor {
    operation_visitor(golos::chain::database &db, const golos::chain::operation_notification &note, const golos::chain::operation_object *&n, std::string i)
        : _db(db), _note(note), new_obj(n), item(i){};

    typedef void result_type;

    golos::chain::database &_db;
    const golos::chain::operation_notification &_note;
    const golos::chain::operation_object *&new_obj;
    string item;

    template<typename Op>
    void operator()(Op &&) const {
        const auto &hist_idx = _db.get_index<golos::chain::account_history_index>().indices().get<golos::chain::by_account>();
        if (!new_obj) {
            new_obj = &_db.create<golos::chain::operation_object>([&](golos::chain::operation_object &obj) {
                obj.trx_id = _note.trx_id;
                obj.block = _note.block;
                obj.trx_in_block = _note.trx_in_block;
                obj.op_in_trx = _note.op_in_trx;
                obj.virtual_op = _note.virtual_op;
                obj.timestamp = _db.head_block_time();
                //fc::raw::pack( obj.serialized_op , _note.op);  //call to 'pack' is ambiguous
                auto size = fc::raw::pack_size(_note.op);
                obj.serialized_op.resize(size);
                fc::datastream<char *> ds(obj.serialized_op.data(), size);
                fc::raw::pack(ds, _note.op);
            });
        }

        auto hist_itr = hist_idx.lower_bound(boost::make_tuple(item, uint32_t(-1)));
        uint32_t sequence = 0;
        if (hist_itr != hist_idx.end() && hist_itr->account == item) {
                sequence = hist_itr->sequence + 1;
        }

        _db.create<golos::chain::account_history_object>([&](golos::chain::account_history_object &ahist) {
            ahist.account = item;
            ahist.sequence = sequence;
            ahist.op = new_obj->id;
        });
    }
};
struct operation_visitor_filter : operation_visitor {
    operation_visitor_filter(golos::chain::database &db, const golos::chain::operation_notification &note, const golos::chain::operation_object *&n, std::string i, const flat_set<string> &filter, bool blacklist, uint32_t start_block)
        : operation_visitor(db, note, n, i), _filter(filter), _blacklist(blacklist), _start_block(start_block) {
    }

    const flat_set<string> &_filter;
    bool _blacklist;
    uint32_t _start_block;

    template <typename T>
    void operator()(const T &op) const {
        if(_db.head_block_num() >= _start_block) {
            if (_filter.find(fc::get_typename<T>::name()) != _filter.end()) {
                if (!_blacklist)
                    operation_visitor::operator()(op);
            } else {
                if (_blacklist)
                    operation_visitor::operator()(op);
            }
        }
    }
};

using namespace golos::protocol;

struct plugin::plugin_impl {
public:
    plugin_impl( ) : database_(appbase::app().get_plugin<chain::plugin>().db()) {
    }

    ~plugin_impl() = default;

    auto database() -> golos::chain::database & {
        return database_;
    }

    // void on_operation(const golos::chain::operation_notification &note);
    void on_operation(const golos::chain::operation_notification &note) {
        flat_set<golos::chain::account_name_type> impacted;
        golos::chain::database &db = database();

        const golos::chain::operation_object *new_obj = nullptr;
        operation_get_impacted_accounts(note.op, impacted);

        for (const auto &item : impacted) {
            auto itr = _tracked_accounts.lower_bound(item);
            if (!_tracked_accounts.size() ||
                (itr != _tracked_accounts.end() && itr->first <= item && item <= itr->second)) {
                if (_filter_content) {
                    note.op.visit(operation_visitor_filter(db, note, new_obj, item, _op_list, _blacklist, _start_block));
                } else {
                    note.op.visit(operation_visitor(db, note, new_obj, item));
                }
            }
        }
    }

    flat_map<string, string> _tracked_accounts;
    bool _filter_content = false;
    uint32_t _start_block = 0;
    bool _blacklist = false;
    flat_set<string> _op_list;
    golos::chain::database &database_;
};





struct get_impacted_account_visitor {
    flat_set<golos::chain::account_name_type> &_impacted;

    get_impacted_account_visitor(flat_set<golos::chain::account_name_type> &impact)
            : _impacted(impact) {
    }

    typedef void result_type;

    template<typename T>
    void operator()(const T &op) {
        op.get_required_posting_authorities(_impacted);
        op.get_required_active_authorities(_impacted);
        op.get_required_owner_authorities(_impacted);
    }

    void operator()(const account_create_operation &op) {
        _impacted.insert(op.new_account_name);
        _impacted.insert(op.creator);
    }

    void operator()(const account_update_operation &op) {
        _impacted.insert(op.account);
    }

    void operator()(const comment_operation &op) {
        _impacted.insert(op.author);
        if (op.parent_author.size()) {
            _impacted.insert(op.parent_author);
        }
    }

    void operator()(const delete_comment_operation &op) {
        _impacted.insert(op.author);
    }

    void operator()(const vote_operation &op) {
        _impacted.insert(op.voter);
        _impacted.insert(op.author);
    }

    void operator()(const author_reward_operation &op) {
        _impacted.insert(op.author);
    }

    void operator()(const curation_reward_operation &op) {
        _impacted.insert(op.curator);
    }

    void operator()(const liquidity_reward_operation &op) {
        _impacted.insert(op.owner);
    }

    void operator()(const interest_operation &op) {
        _impacted.insert(op.owner);
    }

    void operator()(const fill_convert_request_operation &op) {
        _impacted.insert(op.owner);
    }

    void operator()(const transfer_operation &op) {
        _impacted.insert(op.from);
        _impacted.insert(op.to);
    }

    void operator()(const transfer_to_vesting_operation &op) {
        _impacted.insert(op.from);

        if (op.to != golos::chain::account_name_type() && op.to != op.from) {
            _impacted.insert(op.to);
        }
    }

    void operator()(const withdraw_vesting_operation &op) {
        _impacted.insert(op.account);
    }

    void operator()(const witness_update_operation &op) {
        _impacted.insert(op.owner);
    }

    void operator()(const account_witness_vote_operation &op) {
        _impacted.insert(op.account);
        _impacted.insert(op.witness);
    }

    void operator()(const account_witness_proxy_operation &op) {
        _impacted.insert(op.account);
        _impacted.insert(op.proxy);
    }

    void operator()(const feed_publish_operation &op) {
        _impacted.insert(op.publisher);
    }

    void operator()(const limit_order_create_operation &op) {
        _impacted.insert(op.owner);
    }

    void operator()(const fill_order_operation &op) {
        _impacted.insert(op.current_owner);
        _impacted.insert(op.open_owner);
    }

    void operator()(const limit_order_cancel_operation &op) {
        _impacted.insert(op.owner);
    }

    void operator()(const pow_operation &op) {
        _impacted.insert(op.worker_account);
    }

    void operator()(const fill_vesting_withdraw_operation &op) {
        _impacted.insert(op.from_account);
        _impacted.insert(op.to_account);
    }

    void operator()(const shutdown_witness_operation &op) {
        _impacted.insert(op.owner);
    }

    void operator()(const custom_operation &op) {
        for (auto s: op.required_auths) {
            _impacted.insert(s);
        }
    }

    void operator()(const request_account_recovery_operation &op) {
        _impacted.insert(op.account_to_recover);
    }

    void operator()(const recover_account_operation &op) {
        _impacted.insert(op.account_to_recover);
    }

    void operator()(const change_recovery_account_operation &op) {
        _impacted.insert(op.account_to_recover);
    }

    void operator()(const escrow_transfer_operation &op) {
        _impacted.insert(op.from);
        _impacted.insert(op.to);
        _impacted.insert(op.agent);
    }

    void operator()(const escrow_approve_operation &op) {
        _impacted.insert(op.from);
        _impacted.insert(op.to);
        _impacted.insert(op.agent);
    }

    void operator()(const escrow_dispute_operation &op) {
        _impacted.insert(op.from);
        _impacted.insert(op.to);
        _impacted.insert(op.agent);
    }

    void operator()(const escrow_release_operation &op) {
        _impacted.insert(op.from);
        _impacted.insert(op.to);
        _impacted.insert(op.agent);
    }

    void operator()(const transfer_to_savings_operation &op) {
        _impacted.insert(op.from);
        _impacted.insert(op.to);
    }

    void operator()(const transfer_from_savings_operation &op) {
        _impacted.insert(op.from);
        _impacted.insert(op.to);
    }

    void operator()(const cancel_transfer_from_savings_operation &op) {
        _impacted.insert(op.from);
    }

    void operator()(const decline_voting_rights_operation &op) {
        _impacted.insert(op.account);
    }

    void operator()( const comment_benefactor_reward_operation& op ) {
        _impacted.insert( op.benefactor );
        _impacted.insert( op.author );
    }

    //void operator()( const operation& op ){}
};

void operation_get_impacted_accounts(const operation &op, flat_set<golos::chain::account_name_type> &result) {
    get_impacted_account_visitor vtor = get_impacted_account_visitor(result);
    op.visit(vtor);
}

void plugin::set_program_options(
        boost::program_options::options_description &cli,
        boost::program_options::options_description &cfg
) {
    cli.add_options()
         ("track-account-range", boost::program_options::value< vector< string > >()->composing()->multitoken(), "Defines a range of accounts to track as a json pair [\"from\",\"to\"] [from,to] Can be specified multiple times")
         ("history-whitelist-ops", boost::program_options::value< vector< string > >()->composing(), "Defines a list of operations which will be explicitly logged.")
         ("history-blacklist-ops", boost::program_options::value< vector< string > >()->composing(), "Defines a list of operations which will be explicitly ignored.")
         ("history-start-block", boost::program_options::value<uint32_t>()->composing(), "Defines starting block from which recording stats.")
         ;
    cfg.add(cli);
}

void plugin::plugin_initialize(const boost::program_options::variables_map &options) {
    ilog("account_history plugin: plugin_initialize() begin");
    my.reset(new plugin_impl);
    // auto & tmp_db_ref = appbase::app().get_plugin<chain::plugin>().db();
    my->database().pre_apply_operation.connect([&](const golos::chain::operation_notification &note) { my->on_operation(note); });

    typedef pair<string, string> pairstring;
    LOAD_VALUE_SET(options, "track-account-range", my->_tracked_accounts, pairstring);

    if (options.count("history-whitelist-ops")) {
        my->_filter_content = true;
        my->_blacklist = false;

        for (auto &arg : options.at("history-whitelist-ops").as<vector<string>>()) {
            vector<string> ops;
            boost::split(ops, arg, boost::is_any_of(" \t,"));

            for (const string &op : ops) {
                if (op.size())
                    my->_op_list.insert(STEEM_NAMESPACE_PREFIX + op);
            }
        }

        ilog("Account History: whitelisting ops ${o}", ("o", my->_op_list));
    }
    else if (options.count("history-blacklist-ops")) {
        my->_filter_content = true;
        my->_blacklist = true;
        for (auto &arg : options.at("history-blacklist-ops").as<vector<string>>()) {
            vector<string> ops;
            boost::split(ops, arg, boost::is_any_of(" \t,"));

            for (const string &op : ops) {
                if (op.size())
                    my->_op_list.insert(STEEM_NAMESPACE_PREFIX + op);
            }
        }

        ilog("Account History: blacklisting ops ${o}", ("o", my->_op_list));
    }

    if (options.count("history-start-block")) {
        my->_filter_content = true;
        my->_start_block = options.at("history-start-block").as<uint32_t>();
    }
    else {
        my->_start_block = 0;
    }
    ilog("Account History: start_block ${s}", ("s", my->_start_block));
    ilog("account_history plugin: plugin_initialize() end");
    // init(options);
}
plugin::plugin ( ) {

}
plugin::~plugin ( ) {

}
void plugin::plugin_startup() {
    ilog("account_history plugin: plugin_startup() begin");

    ilog("account_history plugin: plugin_startup() end");
}

flat_map<string, string> plugin::tracked_accounts() const {
    return my->_tracked_accounts;
}

} } } // golos::plugins::account_history
