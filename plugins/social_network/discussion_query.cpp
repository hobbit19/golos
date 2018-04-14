

#include <boost/algorithm/string.hpp>
#include <golos/plugins/social_network/api_object/discussion_query.hpp>

namespace golos { namespace plugins { namespace social_network {

    void tags_to_lower(std::set<std::string>& tags) {
        auto src = std::move(tags);
        for (const auto& name: src) {
            auto value = boost::trim_copy(name);
            boost::to_lower(value);
            if (!value.empty()) {
                tags.insert(value);
            }
        }
    }

    void discussion_query::prepare() {
        tags_to_lower(select_tags);
        tags_to_lower(filter_tags);
        tags_to_lower(select_languages);
        tags_to_lower(filter_languages);
    }

    void discussion_query::validate() const {
        FC_ASSERT(limit <= 100);

        for (auto& itr : filter_tags) {
            FC_ASSERT(select_tags.find(itr) == select_tags.end());
        }

        for (auto& itr : filter_languages) {
            FC_ASSERT(select_languages.find(itr) == select_languages.end());
        }

        FC_ASSERT(start_author.valid() == start_permlink.valid());

        FC_ASSERT(parent_author.valid() == parent_permlink.valid());
    }
} } } // golos::plugins::social_network

