#pragma once

#include "tags_object.hpp"
#include <golos/chain/comment_object.hpp>
#include <golos/chain/account_object.hpp>
#include <boost/algorithm/string.hpp>

namespace golos {
    namespace plugins {
        namespace social_network {
            namespace tags {

                struct operation_visitor {
                    operation_visitor(database &db);;
                    typedef void result_type;

                    database &_db;

                    void remove_stats(const tag_object &tag, const tag_stats_object &stats) const;

                    void add_stats(const tag_object &tag, const tag_stats_object &stats) const;

                    void remove_tag(const tag_object &tag) const;

                    const tag_stats_object &get_stats(const std::string &tag) const;

                    comment_metadata filter_tags(const comment_object &c) const;


                    void update_tag(const tag_object &current, const comment_object &comment, double hot, double trending) const;

                    void create_tag(const std::string &tag, const comment_object &comment, double hot, double trending) const;

                    /**
                     * https://medium.com/hacking-and-gonzo/how-reddit-ranking-algorithms-work-ef111e33d0d9#.lcbj6auuw
                     */
                    template<int64_t S, int32_t T>
                    double calculate_score(const share_type &score, const time_point_sec &created) const {
                        /// new algorithm
                        auto mod_score = score.value / S;

                        /// reddit algorithm
                        double order = log10(std::max<int64_t>(std::abs(mod_score), 1));
                        int sign = 0;

                        if (mod_score > 0) {
                            sign = 1;
                        } else if (mod_score < 0) {
                            sign = -1;
                        }

                        return sign * order + double(created.sec_since_epoch()) / double(T);
                    }

                    inline double calculate_hot(const share_type &score, const time_point_sec &created) const;

                    inline double calculate_trending(const share_type &score, const time_point_sec &created) const;


                    /** finds tags that have been added or removed or updated */
                    void update_tags(const comment_object &c, bool parse_tags = false) const;

                    const peer_stats_object &get_or_create_peer_stats(account_object::id_type voter,
                                                                      account_object::id_type peer) const;

                    void update_indirect_vote(account_object::id_type a, account_object::id_type b, int positive) const;

                    void update_peer_stats(const account_object &voter, const account_object &author,
                                           const comment_object &c, int vote) const;


                    void operator()(const comment_operation &op) const;

                    void operator()(const transfer_operation &op) const;

                    void operator()(const vote_operation &op) const;

                    void operator()(const delete_comment_operation &op) const;

                    void operator()(const comment_reward_operation &op) const;

                    void operator()(const comment_payout_update_operation &op) const;

                    template<typename Op>
                    void operator()(Op &&) const {
                    } /// ignore all other ops
                };

            }
        }
    }
}
