#include <boost/program_options/options_description.hpp>
#include <golos/plugins/social_network/social_network.hpp>
#include <golos/plugins/social_network/tag/tags_object.hpp>
#include <golos/plugins/social_network/languages/language_object.hpp>
#include <golos/chain/index.hpp>
#include <golos/plugins/social_network/api_object/discussion.hpp>
#include <golos/plugins/social_network/api_object/discussion_query.hpp>
#include <golos/plugins/social_network/api_object/vote_state.hpp>
#include <golos/plugins/social_network/languages/language_object.hpp>
#include <golos/chain/steem_objects.hpp>

// These visitors creates additional tables, we don't really need them in LOW_MEM mode
#include <golos/plugins/social_network/tag/tag_visitor.hpp>
#include <golos/plugins/social_network/languages/language_visitor.hpp>
#include <golos/chain/operation_notification.hpp>

#define CHECK_ARG_SIZE(_S)                                 \
   FC_ASSERT(                                              \
       args.args->size() == _S,                            \
       "Expected #_S argument(s), was ${n}",               \
       ("n", args.args->size()) );

#define CHECK_ARG_MIN_SIZE(_S, _M)                         \
   FC_ASSERT(                                              \
       args.args->size() >= _S && args.args->size() <= _M, \
       "Expected #_S (maximum #_M) argument(s), was ${n}", \
       ("n", args.args->size()) );

#define GET_OPTIONAL_ARG(_I, _T, _D)   \
   (args.args->size() > _I) ?          \
   (args.args->at(_I).as<_T>()) :      \
   static_cast<_T>(_D)

namespace golos { namespace plugins { namespace social_network {

    using golos::chain::feed_history_object;

    struct social_network::impl final {
        impl(): database_(appbase::app().get_plugin<chain::plugin>().db()) {}

        ~impl() {}

        void on_operation(const operation_notification& note) {
#ifndef IS_LOW_MEM
            try {
                /// plugins shouldn't ever throw
                note.op.visit(languages::operation_visitor(database(), cache_languages));
                note.op.visit(tags::operation_visitor(database()));
            } catch (const fc::exception& e) {
                edump((e.to_detail_string()));
            } catch (...) {
                elog("unhandled exception");
            }
#endif
        }

        golos::chain::database& database() {
            return database_;
        }

        golos::chain::database& database() const {
            return database_;
        }

        share_type get_account_reputation(const account_name_type& account) const {
            auto& db = database();

            if (!db.has_index<follow::reputation_index>()) {
                return 0;
            }

            auto& rep_idx = db.get_index<follow::reputation_index>().indices().get<follow::by_account>();
            auto itr = rep_idx.find(account);

            if (rep_idx.end() != itr) {
                return itr->reputation;
            }

            return 0;
        }

        void select_active_votes(
            std::vector<vote_state>& result, uint32_t& total_count,
            const std::string& author, const std::string& permlink, uint32_t limit
        ) const {
            const auto& comment = database().get_comment(author, permlink);
            const auto& idx = database().get_index<comment_vote_index>().indices().get<by_comment_voter>();
            comment_object::id_type cid(comment.id);
            total_count = 0;
            result.clear();
            for (auto itr = idx.lower_bound(cid); itr != idx.end() && itr->comment == cid; ++itr, ++total_count) {
                if (result.size() < limit) {
                    const auto& vo = database().get(itr->voter);
                    vote_state vstate;
                    vstate.voter = vo.name;
                    vstate.weight = itr->weight;
                    vstate.rshares = itr->rshares;
                    vstate.percent = itr->vote_percent;
                    vstate.time = itr->last_update;
                    vstate.reputation = get_account_reputation(vo.name);
                    result.emplace_back(vstate);
                }
            }
        }

        bool filter_tags(discussion_query& query) const;

        bool filter_languages(discussion_query& query) const;

        std::set<account_object::id_type> filter_authors(const discussion_query& query) const;

        template<
            typename DatabaseIndex,
            typename CommentIndex,
            typename DiscussionIndex,
            typename Getter>
        std::vector<discussion> select_unordered_discussions(
            discussion_query& query,
            Getter&& getter
        ) const;

        template<
            typename DatabaseIndex,
            typename CommentIndex,
            typename DiscussionIndex,
            typename Selector>
        std::vector<discussion> select_ordered_discussions(
            discussion_query& query,
            const std::set<std::string>& select_set,
            const std::set<std::string>& filter_set,
            Selector&& selector
        ) const;

        template<
            typename TagsIndex,
            typename LanguageIndex,
            typename Selector>
        std::vector<discussion> select_ordered_discussions(discussion_query&, Selector&&) const;

        void select_content_replies(
            std::vector<discussion>& result, const std::string& author, const std::string& permlink, uint32_t limit
        ) const;

        std::vector<discussion> get_content_replies(
            const std::string& author, const std::string& permlink, uint32_t vote_limit
        ) const;

        std::vector<discussion> get_all_content_replies(
            const std::string& author, const std::string& permlink, uint32_t vote_limit
        ) const;

        std::vector<tag_api_object> get_trending_tags(std::string after, uint32_t limit) const;

        std::vector<std::pair<std::string, uint32_t>> get_tags_used_by_author(const std::string& author) const;

        void set_pending_payout(discussion& d) const;

        void set_url(discussion& d) const;

        std::vector<discussion> get_replies_by_last_update(
            account_name_type start_parent_author, std::string start_permlink,
            uint32_t limit, uint32_t vote_limit
        ) const;

        discussion get_content(std::string author, std::string permlink, uint32_t limit) const;

        discussion get_discussion(const comment_object& c, uint32_t vote_limit) const {
            discussion d(c);
            set_url(d);
            set_pending_payout(d);
            select_active_votes(d.active_votes, d.active_votes_count, d.author, d.permlink, vote_limit);
            return d;
        }

        void fill_discussion(discussion& d, const discussion_query& query) const;

        get_languages_r get_languages();

        std::set<std::string> cache_languages;

    private:
        golos::chain::database& database_;
    };


    get_languages_r social_network::impl::get_languages() {
        get_languages_r result;
        result.languages = cache_languages;
        return result;
    }

    void social_network::impl::fill_discussion(discussion& d, const discussion_query& query) const {
        set_url(d);
        set_pending_payout(d);
        select_active_votes(d.active_votes, d.active_votes_count, d.author, d.permlink, query.vote_limit);
        d.body_length = static_cast<uint32_t>(d.body.size());
        if (query.truncate_body) {
            d.body.erase(query.truncate_body);

            if (!fc::is_utf8(d.title)) {
                d.title = fc::prune_invalid_utf8(d.title);
            }

            if (!fc::is_utf8(d.body)) {
                d.body = fc::prune_invalid_utf8(d.body);
            }

            if (!fc::is_utf8(d.category)) {
                d.category = fc::prune_invalid_utf8(d.category);
            }

            if (!fc::is_utf8(d.json_metadata)) {
                d.json_metadata = fc::prune_invalid_utf8(d.json_metadata);
            }
        }
    }

    DEFINE_API(social_network, get_languages) {
        auto& db = pimpl->database();
        return db.with_weak_read_lock([&]() {
            get_languages_r result;
            result = pimpl->get_languages();
            return result;
        });
    }

    void social_network::plugin_startup() {
    }

    void social_network::plugin_shutdown() {
    }

    const std::string& social_network::name() {
        static std::string name = "social_network";
        return name;
    }

    social_network::social_network() {
    }

    void social_network::set_program_options(
        boost::program_options::options_description&,
        boost::program_options::options_description& config_file_options
    ) {
    }

    void social_network::plugin_initialize(const boost::program_options::variables_map& options) {
        pimpl.reset(new impl());
// Disable index creation for tag and language visitors
#ifndef IS_LOW_MEM
        auto& db = pimpl->database();
        db.post_apply_operation.connect([&](const operation_notification& note) {
            pimpl->on_operation(note);
        });
        add_plugin_index<tags::tag_index>(db);
        add_plugin_index<tags::tag_stats_index>(db);
        add_plugin_index<tags::peer_stats_index>(db);
        add_plugin_index<tags::author_tag_stats_index>(db);

        add_plugin_index<languages::language_index>(db);
        add_plugin_index<languages::language_stats_index>(db);
        add_plugin_index<languages::peer_stats_index>(db);
        add_plugin_index<languages::author_language_stats_index>(db);
#endif
        JSON_RPC_REGISTER_API (name());

    }

    social_network::~social_network() = default;

    DEFINE_API(social_network, get_active_votes) {
        CHECK_ARG_MIN_SIZE(2, 3)
        auto author = args.args->at(0).as<string>();
        auto permlink = args.args->at(1).as<string>();
        auto limit = GET_OPTIONAL_ARG(2, uint32_t, DEFAULT_VOTE_LIMIT);
        return pimpl->database().with_weak_read_lock([&]() {
            std::vector<vote_state> result;
            uint32_t total_count;
            pimpl->select_active_votes(result, total_count, author, permlink, limit);
            return result;
        });
    }

    void social_network::impl::set_url(discussion& d) const {
        const comment_api_object root(database().get<comment_object, by_id>(d.root_comment));
        d.url = "/" + root.category + "/@" + root.author + "/" + root.permlink;
        d.root_title = root.title;
        if (root.id != d.id) {
            d.url += "#@" + d.author + "/" + d.permlink;
        }
    }

    void social_network::impl::select_content_replies(
        std::vector<discussion>& result, const std::string& author, const std::string& permlink, uint32_t limit
    ) const {
        account_name_type acc_name = account_name_type(author);
        const auto& by_permlink_idx = database().get_index<comment_index>().indices().get<by_parent>();
        auto itr = by_permlink_idx.find(boost::make_tuple(acc_name, permlink));
        while (
            itr != by_permlink_idx.end() &&
            itr->parent_author == author &&
            to_string(itr->parent_permlink) == permlink
        ) {
            result.emplace_back(get_discussion(*itr, limit));
            ++itr;
        }
    }

    std::vector<discussion> social_network::impl::get_content_replies(
        const std::string& author, const std::string& permlink, uint32_t vote_limit
    ) const {
        std::vector<discussion> result;
        select_content_replies(result, author, permlink, vote_limit);
        return result;
    }

    DEFINE_API(social_network, get_content_replies) {
        CHECK_ARG_MIN_SIZE(2, 3)
        auto author = args.args->at(0).as<string>();
        auto permlink = args.args->at(1).as<string>();
        auto vote_limit = GET_OPTIONAL_ARG(2, uint32_t, DEFAULT_VOTE_LIMIT);
        return pimpl->database().with_weak_read_lock([&]() {
            return pimpl->get_content_replies(author, permlink, vote_limit);
        });
    }

    std::vector<discussion> social_network::impl::get_all_content_replies(
        const std::string& author, const std::string& permlink, uint32_t vote_limit
    ) const {
        std::vector<discussion> result;
        select_content_replies(result, author, permlink, vote_limit);
        for (std::size_t i = 0; i < result.size(); ++i) {
            if (result[i].children > 0) {
                auto j = result.size();
                select_content_replies(result, result[i].author, result[i].permlink, vote_limit);
                for (; j < result.size(); ++j) {
                    result[i].replies.push_back(result[j].author + "/" + result[j].permlink);
                }
            }
        }
        return result;
    }

    DEFINE_API(social_network, get_all_content_replies) {
        CHECK_ARG_MIN_SIZE(2, 3)
        auto author = args.args->at(0).as<string>();
        auto permlink = args.args->at(1).as<string>();
        auto vote_limit = GET_OPTIONAL_ARG(2, uint32_t, DEFAULT_VOTE_LIMIT);
        return pimpl->database().with_weak_read_lock([&]() {
            return pimpl->get_all_content_replies(author, permlink, vote_limit);
        });
    }

    boost::multiprecision::uint256_t to256(const fc::uint128_t& t) {
        boost::multiprecision::uint256_t result(t.high_bits());
        result <<= 65;
        result += t.low_bits();
        return result;
    }


    void social_network::impl::set_pending_payout(discussion& d) const {
        auto& db = database();
#ifndef IS_LOW_MEM
        const auto& cidx = db.get_index<tags::tag_index>().indices().get<tags::by_comment>();
        auto itr = cidx.lower_bound(d.id);
        if (itr != cidx.end() && itr->comment == d.id) {
            d.promoted = asset(itr->promoted_balance, SBD_SYMBOL);
        }
#endif
        const auto& props = db.get_dynamic_global_properties();
        const auto& hist = db.get_feed_history();
        asset pot = props.total_reward_fund_steem;
        if (!hist.current_median_history.is_null()) {
            pot = pot * hist.current_median_history;
        }

        u256 total_r2 = to256(props.total_reward_shares2);

        if (props.total_reward_shares2 > 0) {
            auto vshares = db.calculate_vshares(d.net_rshares.value > 0 ? d.net_rshares.value : 0);

            u256 r2 = to256(vshares); //to256(abs_net_rshares);
            r2 *= pot.amount.value;
            r2 /= total_r2;

            u256 tpp = to256(d.children_rshares2);
            tpp *= pot.amount.value;
            tpp /= total_r2;

            d.pending_payout_value = asset(static_cast<uint64_t>(r2), pot.symbol);
            d.total_pending_payout_value = asset(static_cast<uint64_t>(tpp), pot.symbol);

            d.author_reputation = get_account_reputation(d.author);

        }

        if (d.parent_author != STEEMIT_ROOT_POST_PARENT) {
            d.cashout_time = db.calculate_discussion_payout_time(db.get<comment_object>(d.id));
        }

        if (d.body.size() > 1024 * 128) {
            d.body = "body pruned due to size";
        }
        if (d.parent_author.size() > 0 && d.body.size() > 1024 * 16) {
            d.body = "comment pruned due to size";
        }

        set_url(d);
    }

    DEFINE_API(social_network, get_trending_categories) {
        CHECK_ARG_SIZE(2)
        auto after = args.args->at(0).as<string>();
        auto limit = args.args->at(1).as<uint32_t>();
        auto& db = pimpl->database();
        return db.with_weak_read_lock([&]() {
            limit = std::min(limit, uint32_t(100));
            std::vector<category_api_object> result;
            result.reserve(limit);

            const auto& nidx = db.get_index<golos::chain::category_index>().indices().get<by_name>();
            const auto& ridx = db.get_index<golos::chain::category_index>().indices().get<by_rshares>();
            auto itr = ridx.begin();
            if (after != "" && nidx.size()) {
                auto nitr = nidx.lower_bound(after);
                if (nitr == nidx.end()) {
                    itr = ridx.end();
                } else {
                    itr = ridx.iterator_to(*nitr);
                }
            }

            while (itr != ridx.end() && result.size() < limit) {
                result.emplace_back(category_api_object(*itr));
                ++itr;
            }
            return result;
        });
    }

    DEFINE_API(social_network, get_best_categories) {
        CHECK_ARG_SIZE(2)
        auto after = args.args->at(0).as<string>();
        auto limit = args.args->at(1).as<uint32_t>();
        return pimpl->database().with_weak_read_lock([&]() {
            limit = std::min(limit, uint32_t(100));
            std::vector<category_api_object> result;
            result.reserve(limit);
            return result;
        });
    }

    DEFINE_API(social_network, get_active_categories) {
        CHECK_ARG_SIZE(2)
        auto after = args.args->at(0).as<string>();
        auto limit = args.args->at(1).as<uint32_t>();
        return pimpl->database().with_weak_read_lock([&]() {
            limit = std::min(limit, uint32_t(100));
            std::vector<category_api_object> result;
            result.reserve(limit);
            return result;
        });
    }

    DEFINE_API(social_network, get_recent_categories) {
        CHECK_ARG_SIZE(2)
        auto after = args.args->at(0).as<string>();
        auto limit = args.args->at(1).as<uint32_t>();
        return pimpl->database().with_weak_read_lock([&]() {
            limit = std::min(limit, uint32_t(100));
            std::vector<category_api_object> result;
            result.reserve(limit);
            return result;
        });
    }

    inline bool is_good_language(const discussion_query& query, const discussion& d) {
        if (query.select_languages.empty() && query.filter_languages.empty()) {
            return true;
        }

        auto language = languages::get_language(d);

        if (!query.select_languages.empty() && !query.select_languages.count(language)) {
            return false;
        }

        if (!query.filter_languages.empty() && query.filter_languages.count(language)) {
            return false;
        }

        return true;
    }

    inline bool is_good_tags(const discussion_query& query, const discussion& d) {
        if (query.select_tags.empty() && query.filter_tags.empty()) {
            return true;
        }

        auto tags = tags::get_tags(d);
        bool result = query.select_tags.empty();
        for (auto& t: tags) {
            if (!query.filter_tags.empty() && query.filter_tags.count(t)) {
                return false;
            } else if (!result && query.select_tags.count(t)) {
                result = true;
            }
        }

        return result;
    }

    bool social_network::impl::filter_tags(discussion_query& query) const {
        if (query.select_tags.empty()) {
            return true;
        }

        auto& db = database();
        auto& idx = db.get_index<tags::tag_index>().indices().get<tags::by_parent_created>();
        std::set<std::string> exists_tags;
        for (auto& t: query.select_tags) {
            if (idx.find(t) != idx.end()) {
                exists_tags.insert(t);
            }
        }
        if (exists_tags.empty()) {
            return false;
        }

        query.select_tags.swap(exists_tags);
        return true;
    }

    bool social_network::impl::filter_languages(discussion_query& query) const {
        if (query.select_languages.empty()) {
            return true;
        }

        auto& db = database();
        auto& idx = db.get_index<languages::language_index>().indices().get<languages::by_parent_created>();
        std::set<std::string> exists_languages;
        for (auto& t: query.select_languages) {
            if (idx.find(t) != idx.end()) {
                exists_languages.insert(t);
            }
        }
        if (exists_languages.empty()) {
            return false;
        }

        query.select_languages.swap(exists_languages);
        return true;
    }

    std::set<account_object::id_type> social_network::impl::filter_authors(const discussion_query& query) const {
        auto& db = database();
        std::set<account_object::id_type> exists_authors;
        for (auto& name: query.select_authors) {
            auto* author = db.find_account(name);
            if (author) {
                exists_authors.insert(author->id);
            }
        }
        return exists_authors;
    }

    template<
        typename DatabaseIndex,
        typename CommentIndex,
        typename DiscussionIndex,
        typename Getter>
    std::vector<discussion> social_network::impl::select_unordered_discussions(
        discussion_query& query,
        Getter&& getter
    ) const {
        std::vector<discussion> result;

        // check existing tags and languages
        if (!filter_tags(query) || !filter_languages(query)) {
            return result;
        }

        auto& db = database();

        // author_set is not used in select for-loop, but it allows to skip selecting if all nicks doesn't exist
        auto author_set = filter_authors(query);
        if (query.select_authors.empty() != author_set.empty()) {
            return result;
        }

        const auto& indices = db.get_index<DatabaseIndex>().indices();
        const auto& idx = indices.template get<DiscussionIndex>();
        auto itr = idx.begin();
        // seek to start comment
        if (query.start_author && query.start_permlink) {
            const auto* comment = db.find_comment(*query.start_author, *query.start_permlink);
            if (!comment) {
                return result;
            }

            const auto& cidx = indices.template get<CommentIndex>();
            const auto citr = cidx.find(comment->id);
            if (citr == cidx.end()) {
                return result;
            }

            itr = idx.iterator_to(*citr);
        }

        result.reserve(query.limit);

        uint32_t depth = 0;
        std::set<comment_object::id_type> id_set;
        auto id_limit = query.start + query.limit;
        for (auto etr = idx.end(); itr != etr && id_set.size() < id_limit && depth < query.depth; ++itr, ++depth) {
            const auto* comment = getter(*itr);

            if (!comment ||
                id_set.count(comment->id) ||
                (!query.select_authors.empty() && !query.select_authors.count(comment->author)) ||
                (query.parent_author && *query.parent_author != comment->parent_author) ||
                (query.parent_permlink && *query.parent_permlink != to_string(comment->parent_permlink))
            ) {
                continue;
            }

            discussion d(*comment);
            if (!is_good_language(query, d) || !is_good_tags(query, d)) {
                continue;
            }

            id_set.insert(d.id);
            if (id_set.size() <= query.start) {
                continue;
            }

            fill_discussion(d, query);
            result.push_back(d);
        }
        return result;
    }

    template<
        typename DatabaseIndex,
        typename CommentIndex,
        typename DiscussionIndex,
        typename Selector>
    std::vector<discussion> social_network::impl::select_ordered_discussions(
        discussion_query& query,
        const std::set<std::string>& select_set,
        const std::set<std::string>& filter_set,
        Selector&& selector
    ) const {
        std::vector<discussion> result;

        // check existing tags and languages
        if (!filter_tags(query) || !filter_languages(query)) {
            return result;
        }

        auto& db = database();

        auto author_set = filter_authors(query);
        // check existing of authors
        if (query.select_authors.empty() != author_set.empty()) {
            return result;
        }

        const auto& indices = db.get_index<DatabaseIndex>().indices();
        const auto& idx = indices.template get<DiscussionIndex>();
        auto itr = idx.begin();
        // seek to start comment
        if (query.start_author && query.start_permlink) {
            const auto* comment = db.find_comment(*query.start_author, *query.start_permlink);
            if (!comment) {
                return result;
            }

            const auto& cidx = indices.template get<CommentIndex>();
            const auto citr = cidx.find(comment->id);
            if (citr == cidx.end()) {
                return result;
            }

            itr = idx.iterator_to(*citr);
        }

        comment_object::id_type parent;
        if (query.parent_author && query.parent_permlink) {
            auto* parent_comment = db.find_comment(*query.parent_author, *query.parent_permlink);
            if (parent_comment) {
                return result;
            }
            parent = parent_comment->id;
        }

        result.reserve(query.limit);

        uint32_t depth = 0;
        std::set<comment_object::id_type> id_set;
        auto id_limit = query.start + query.limit;
        for (auto etr = idx.end(); itr != etr && id_set.size() < id_limit && depth < query.depth; ++itr, ++depth) {
            if (id_set.count(itr->comment) ||
                (parent._id && itr->parent != parent) ||
                (!author_set.empty() && !author_set.count(itr->author)) ||
                (!select_set.empty() && !select_set.count(itr->name)) ||
                (!filter_set.empty() && filter_set.count(itr->name))
            ) {
                continue;
            }

            auto* comment = db.find(itr->comment);
            if (!comment) {
                continue;
            }

            discussion d(*comment);
            if (!selector(d) || !is_good_language(query, d) || !is_good_tags(query, d)) {
                continue;
            }

            id_set.insert(d.id);
            if (id_set.size() < query.start) {
                continue;
            }

            fill_discussion(d, query);
            result.push_back(d);
        }
        return result;
    }

    template<
        typename TagsIndex,
        typename LanguagesIndex,
        typename Selector>
    std::vector<discussion> social_network::impl::select_ordered_discussions(
        discussion_query& query,
        Selector&& selector
    ) const {
        if (!query.select_tags.empty()) {
            return select_ordered_discussions<tags::tag_index, tags::by_comment, TagsIndex>(
                query, query.select_tags, query.filter_tags, selector);
        } else {
            return select_ordered_discussions<languages::language_index, languages::by_comment, LanguagesIndex>(
                query, query.select_languages, query.filter_languages, selector);
        }
    }

    DEFINE_API(social_network, get_discussions_by_comments) {
        CHECK_ARG_SIZE(1)
        std::vector<discussion> result;
#ifndef IS_LOW_MEM
        auto query = args.args->at(0).as<discussion_query>();
        query.validate();
        if (query.select_authors.empty()) {
            return result;
        }

        auto& db = pimpl->database();
        return db.with_weak_read_lock([&]() {
            return pimpl->select_unordered_discussions<comment_index, by_id, by_author_last_update>(
                query,
                [&](const comment_object& itr) -> const comment_object* {
                    return &itr;
                }
            );
        });
#endif
        return result;
    }

    DEFINE_API(social_network, get_discussions_by_blog) {
        CHECK_ARG_SIZE(1)
        std::vector<discussion> result;

        auto query = args.args->at(0).as<discussion_query>();
        query.validate();
        if (query.select_authors.empty()) {
            return result;
        }

#ifndef IS_LOW_MEM
        auto& db = pimpl->database();
        FC_ASSERT(db.has_index<follow::feed_index>(), "Node is not running the follow plugin");

        return db.with_weak_read_lock([&]() {
            return pimpl->select_unordered_discussions<follow::blog_index, follow::by_comment, follow::by_blog>(
                query,
                [&](auto itr) -> const comment_object* {
                    return db.find(itr.comment);
                }
            );
        });
#endif
        return result;
    }

    DEFINE_API(social_network, get_discussions_by_feed) {
        CHECK_ARG_SIZE(1)
        std::vector<discussion> result;

        auto query = args.args->at(0).as<discussion_query>();
        query.validate();
        if (query.select_authors.empty()) {
            return result;
        }

#ifndef IS_LOW_MEM
        auto& db = pimpl->database();
        FC_ASSERT(db.has_index<follow::feed_index>(), "Node is not running the follow plugin");

        return db.with_weak_read_lock([&]() {
            return pimpl->select_unordered_discussions<follow::feed_index, follow::by_comment, follow::by_feed>(
                query,
                [&](auto itr) -> const comment_object* {
                    return db.find(itr.comment);
                }
            );
        });
#endif
        return result;
    }

    DEFINE_API(social_network, get_discussions_by_trending) {
        CHECK_ARG_SIZE(1)
        auto query = args.args->at(0).as<discussion_query>();
        query.validate();
#ifndef IS_LOW_MEM
        auto& db = pimpl->database();
        return db.with_weak_read_lock([&]() {
            return pimpl->select_ordered_discussions<tags::by_parent_trending, languages::by_parent_trending>(
                query,
                [&](const discussion& d) -> bool {
                    return d.children_rshares2 > 0 && d.mode != archived;
                }
            );
        });
#endif
        return std::vector<discussion>();
    }

    DEFINE_API(social_network, get_discussions_by_promoted) {
        CHECK_ARG_SIZE(1)
        auto query = args.args->at(0).as<discussion_query>();
        query.validate();
#ifndef IS_LOW_MEM
        auto& db = pimpl->database();
        return db.with_weak_read_lock([&]() {
            return pimpl->select_ordered_discussions<tags::by_parent_promoted, languages::by_parent_promoted>(
                query,
                [&](const discussion& d) -> bool {
                    return d.children_rshares2 > 0;
                }
            );
        });
#endif
        return std::vector<discussion>();
    }

    DEFINE_API(social_network, get_discussions_by_created) {
        CHECK_ARG_SIZE(1)
        auto query = args.args->at(0).as<discussion_query>();
        query.validate();
#ifndef IS_LOW_MEM
        auto& db = pimpl->database();
        return db.with_weak_read_lock([&]() {
            return pimpl->select_ordered_discussions<tags::by_parent_created, languages::by_parent_created>(
                query,
                [&](const discussion& d) -> bool {
                    return true;
                }
            );
        });
#endif
        return std::vector<discussion>();
    }

    DEFINE_API(social_network, get_discussions_by_active) {
        CHECK_ARG_SIZE(1)
        auto query = args.args->at(0).as<discussion_query>();
        query.validate();
#ifndef IS_LOW_MEM
        auto& db = pimpl->database();
        return db.with_weak_read_lock([&]() {
            return pimpl->select_ordered_discussions<tags::by_parent_active, languages::by_parent_active>(
                query,
                [&](const discussion& d) -> bool {
                    return true;
                }
            );
        });
#endif
        return std::vector<discussion>();
    }

    DEFINE_API(social_network, get_discussions_by_cashout) {
        CHECK_ARG_SIZE(1)
        auto query = args.args->at(0).as<discussion_query>();
        query.validate();
#ifndef IS_LOW_MEM
        auto& db = pimpl->database();
        return db.with_weak_read_lock([&]() {
            return pimpl->select_ordered_discussions<tags::by_cashout, languages::by_cashout>(
                query,
                [&](const discussion& d) -> bool {
                    return d.children_rshares2 > 0;
                }
            );
        });
#endif
        return std::vector<discussion>();
    }

    DEFINE_API(social_network, get_discussions_by_payout) {
        CHECK_ARG_SIZE(1)
        auto query = args.args->at(0).as<discussion_query>();
        query.validate();
#ifndef IS_LOW_MEM
        auto& db = pimpl->database();
        return db.with_weak_read_lock([&]() {
            return pimpl->select_ordered_discussions<tags::by_net_rshares, languages::by_net_rshares>(
                query,
                [&](const discussion& d) -> bool {
                    return d.children_rshares2 > 0;
                }
            );
        });
#endif
        return std::vector<discussion>();
    }

    DEFINE_API(social_network, get_discussions_by_votes) {
        CHECK_ARG_SIZE(1)
        auto query = args.args->at(0).as<discussion_query>();
        query.validate();
#ifndef IS_LOW_MEM
        auto& db = pimpl->database();
        return db.with_weak_read_lock([&]() {
            return pimpl->select_ordered_discussions<tags::by_parent_net_votes, languages::by_parent_net_votes>(
                query,
                [&](const discussion& d) -> bool {
                    return d.children_rshares2 > 0;
                }
            );
        });
#endif
        return std::vector<discussion>();
    }

    DEFINE_API(social_network, get_discussions_by_children) {
        CHECK_ARG_SIZE(1)
        auto query = args.args->at(0).as<discussion_query>();
        query.validate();
#ifndef IS_LOW_MEM
        auto& db = pimpl->database();
        return db.with_weak_read_lock([&]() {
            return pimpl->select_ordered_discussions<tags::by_parent_children, languages::by_parent_children>(
                query,
                [&](const discussion& d) -> bool {
                    return true;
                }
            );
        });
#endif
        return std::vector<discussion>();
    }

    DEFINE_API(social_network, get_discussions_by_hot) {
        CHECK_ARG_SIZE(1)
        auto query = args.args->at(0).as<discussion_query>();
        query.validate();
#ifndef IS_LOW_MEM
        auto& db = pimpl->database();
        return db.with_weak_read_lock([&]() {
            return pimpl->select_ordered_discussions<tags::by_parent_hot, languages::by_parent_hot>(
                query,
                [&](const discussion& d) -> bool {
                    return d.children_rshares2 > 0;
                }
            );
        });
#endif
        return std::vector<discussion>();
    }

    DEFINE_API(social_network, get_account_votes) {
        CHECK_ARG_SIZE(1)
        account_name_type voter = args.args->at(0).as<account_name_type>();
        auto& db = pimpl->database();
        return db.with_weak_read_lock([&]() {
            std::vector<account_vote> result;

            const auto& voter_acnt = db.get_account(voter);
            const auto& idx = db.get_index<comment_vote_index>().indices().get<by_voter_comment>();

            account_object::id_type aid(voter_acnt.id);
            auto itr = idx.lower_bound(aid);
            auto end = idx.upper_bound(aid);
            while (itr != end) {
                const auto& vo = db.get(itr->comment);
                account_vote avote;
                avote.authorperm = vo.author + "/" + to_string(vo.permlink);
                avote.weight = itr->weight;
                avote.rshares = itr->rshares;
                avote.percent = itr->vote_percent;
                avote.time = itr->last_update;
                result.emplace_back(avote);
                ++itr;
            }
            return result;
        });
    }

    std::vector<tag_api_object>
    social_network::impl::get_trending_tags(std::string after, uint32_t limit) const {
        limit = std::min(limit, uint32_t(1000));
        std::vector<tag_api_object> result;
#ifndef IS_LOW_MEM
        result.reserve(limit);

        const auto& nidx = database().get_index<tags::tag_stats_index>().indices().get<tags::by_tag>();
        const auto& ridx = database().get_index<tags::tag_stats_index>().indices().get<tags::by_trending>();
        auto itr = ridx.begin();
        if (after != "" && nidx.size()) {
            auto nitr = nidx.lower_bound(after);
            if (nitr == nidx.end()) {
                itr = ridx.end();
            } else {
                itr = ridx.iterator_to(*nitr);
            }
        }

        while (itr != ridx.end() && result.size() < limit) {
            tag_api_object push_object = tag_api_object(*itr);

            if (!fc::is_utf8(push_object.name)) {
                push_object.name = fc::prune_invalid_utf8(push_object.name);
            }

            result.emplace_back(push_object);
            ++itr;
        }
#endif
        return result;
    }

    DEFINE_API(social_network, get_trending_tags) {
        CHECK_ARG_SIZE(2)
        auto after = args.args->at(0).as<string>();
        auto limit = args.args->at(1).as<uint32_t>();

        return pimpl->database().with_weak_read_lock([&]() {
            return pimpl->get_trending_tags(after, limit);
        });
    }

    std::vector<std::pair<std::string, uint32_t>> social_network::impl::get_tags_used_by_author(
        const std::string& author
    ) const {
        std::vector<std::pair<std::string, uint32_t>> result;
#ifndef IS_LOW_MEM
        auto& db = database();
        const auto* acnt = db.find_account(author);
        FC_ASSERT(acnt != nullptr);
        const auto& tidx =
            db.get_index<tags::author_tag_stats_index>().indices().get<tags::by_author_posts_tag>();
        auto itr = tidx.lower_bound(boost::make_tuple(acnt->id, 0));
        while (itr != tidx.end() && itr->author == acnt->id && result.size() < 1000) {
            if (!fc::is_utf8(itr->tag)) {
                result.emplace_back(std::make_pair(fc::prune_invalid_utf8(itr->tag), itr->total_posts));
            } else {
                result.emplace_back(std::make_pair(itr->tag, itr->total_posts));
            }
            ++itr;
        }
#endif
        return result;
    }

    DEFINE_API(social_network, get_tags_used_by_author) {
        CHECK_ARG_SIZE(1)
        auto author = args.args->at(0).as<string>();
        return pimpl->database().with_weak_read_lock([&]() {
            return pimpl->get_tags_used_by_author(author);
        });
    }

    DEFINE_API(social_network, get_discussions_by_author_before_date) {
        std::vector<discussion> result;

        CHECK_ARG_MIN_SIZE(4, 5)
#ifndef IS_LOW_MEM
        auto author = args.args->at(0).as<std::string>();
        auto start_permlink = args.args->at(1).as<std::string>();
        auto before_date = args.args->at(2).as<time_point_sec>();
        auto limit = args.args->at(3).as<uint32_t>();
        auto vote_limit = GET_OPTIONAL_ARG(4, uint32_t, DEFAULT_VOTE_LIMIT);

        FC_ASSERT(limit <= 100);

        result.reserve(limit);

        if (before_date == time_point_sec()) {
            before_date = time_point_sec::maximum();
        }

        auto& db = pimpl->database();

        return db.with_weak_read_lock([&]() {
            try {
                uint32_t count = 0;
                const auto& didx = db.get_index<comment_index>().indices().get<by_author_last_update>();

                auto itr = didx.lower_bound(boost::make_tuple(author, time_point_sec::maximum()));
                if (start_permlink.size()) {
                    const auto& comment = db.get_comment(author, start_permlink);
                    if (comment.created < before_date) {
                        itr = didx.iterator_to(comment);
                    }
                }

                while (itr != didx.end() && itr->author == author && count < limit) {
                    if (itr->parent_author.size() == 0) {
                        result.push_back(pimpl->get_discussion(*itr, vote_limit));
                        ++count;
                    }
                    ++itr;
                }

                return result;
            } FC_CAPTURE_AND_RETHROW((author)(start_permlink)(before_date)(limit))
        });
#endif
        return result;
    }

    discussion social_network::impl::get_content(std::string author, std::string permlink, uint32_t limit) const {
        const auto& by_permlink_idx = database().get_index<comment_index>().indices().get<by_permlink>();
        auto itr = by_permlink_idx.find(boost::make_tuple(author, permlink));
        if (itr != by_permlink_idx.end()) {
            return get_discussion(*itr, limit);
        }
        return discussion();
    }

    DEFINE_API(social_network, get_content) {
        CHECK_ARG_MIN_SIZE(2, 3)
        auto author = args.args->at(0).as<account_name_type>();
        auto permlink = args.args->at(1).as<string>();
        auto vote_limit = GET_OPTIONAL_ARG(2, uint32_t, DEFAULT_VOTE_LIMIT);
        return pimpl->database().with_weak_read_lock([&]() {
            return pimpl->get_content(author, permlink, vote_limit);
        });
    }

    std::vector<discussion> social_network::impl::get_replies_by_last_update(
        account_name_type start_parent_author,
        std::string start_permlink,
        uint32_t limit,
        uint32_t vote_limit
    ) const {
        std::vector<discussion> result;
#ifndef IS_LOW_MEM
        auto& db = database();
        const auto& last_update_idx = db.get_index<comment_index>().indices().get<by_last_update>();
        auto itr = last_update_idx.begin();
        const account_name_type* parent_author = &start_parent_author;

        if (start_permlink.size()) {
            const auto& comment = db.get_comment(start_parent_author, start_permlink);
            itr = last_update_idx.iterator_to(comment);
            parent_author = &comment.parent_author;
        } else if (start_parent_author.size()) {
            itr = last_update_idx.lower_bound(start_parent_author);
        }

        result.reserve(limit);

        while (itr != last_update_idx.end() && result.size() < limit && itr->parent_author == *parent_author) {
            result.emplace_back(get_discussion(*itr, vote_limit));
            ++itr;
        }
#endif
        return result;
    }


    /**
     *  This method can be used to fetch replies to an account.
     *
     *  The first call should be (account_to_retrieve replies, "", limit)
     *  Subsequent calls should be (last_author, last_permlink, limit)
     */
    DEFINE_API(social_network, get_replies_by_last_update) {
        CHECK_ARG_MIN_SIZE(3, 4)
        auto start_parent_author = args.args->at(0).as<account_name_type>();
        auto start_permlink = args.args->at(1).as<string>();
        auto limit = args.args->at(2).as<uint32_t>();
        auto vote_limit = GET_OPTIONAL_ARG(3, uint32_t, DEFAULT_VOTE_LIMIT);
        FC_ASSERT(limit <= 100);
        return pimpl->database().with_weak_read_lock([&]() {
            return pimpl->get_replies_by_last_update(start_parent_author, start_permlink, limit, vote_limit);
        });
    }

} } } // golos::plugins::social_network
