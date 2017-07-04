#pragma once

#include <steemit/protocol/types.hpp>

#include <fc/string.hpp>
#include <fc/time.hpp>

namespace steemit {
    namespace protocol {

/*
 * This class represents the basic versioning scheme of the Golos blockchain.
 * All versions are a triple consisting of a major version, hardfork version, and release version.
 * It allows easy comparison between versions. A version is a read only object.
 */
        struct version {
            version() {
            }

            version(uint8_t m, uint8_t h, uint16_t r);

            virtual ~version() {
            }

            bool operator==(const version &o) const {
                return v_num == o.v_num;
            }

            bool operator!=(const version &o) const {
                return v_num != o.v_num;
            }

            bool operator<(const version &o) const {
                return v_num < o.v_num;
            }

            bool operator<=(const version &o) const {
                return v_num <= o.v_num;
            }

            bool operator>(const version &o) const {
                return v_num > o.v_num;
            }

            bool operator>=(const version &o) const {
                return v_num >= o.v_num;
            }

            operator fc::string() const;

            uint32_t v_num = 0;
        };

        struct hardfork_version : version {
            hardfork_version() : version() {
            }

            hardfork_version(uint8_t m, uint8_t h) : version(m, h, 0) {
            }

            hardfork_version(version v) {
                v_num = v.v_num & 0xFFFF0000;
            }

            ~hardfork_version() {
            }

            void operator=(const version &o) {
                v_num = o.v_num & 0xFFFF0000;
            }

            void operator=(const hardfork_version &o) {
                v_num = o.v_num & 0xFFFF0000;
            }

            bool operator==(const hardfork_version &o) const {
                return v_num == o.v_num;
            }

            bool operator!=(const hardfork_version &o) const {
                return v_num != o.v_num;
            }

            bool operator<(const hardfork_version &o) const {
                return v_num < o.v_num;
            }

            bool operator<=(const hardfork_version &o) const {
                return v_num <= o.v_num;
            }

            bool operator>(const hardfork_version &o) const {
                return v_num > o.v_num;
            }

            bool operator>=(const hardfork_version &o) const {
                return v_num >= o.v_num;
            }

            bool operator==(const version &o) const {
                return v_num == (o.v_num & 0xFFFF0000);
            }

            bool operator!=(const version &o) const {
                return v_num != (o.v_num & 0xFFFF0000);
            }

            bool operator<(const version &o) const {
                return v_num < (o.v_num & 0xFFFF0000);
            }

            bool operator<=(const version &o) const {
                return v_num <= (o.v_num & 0xFFFF0000);
            }

            bool operator>(const version &o) const {
                return v_num > (o.v_num & 0xFFFF0000);
            }

            bool operator>=(const version &o) const {
                return v_num >= (o.v_num & 0xFFFF0000);
            }
        };

        struct hardfork_version_vote {
            hardfork_version_vote() {
            }

            hardfork_version_vote(hardfork_version v, fc::time_point_sec t)
                    : hf_version(v), hf_time(t) {
            }

            hardfork_version hf_version;
            fc::time_point_sec hf_time;
        };

        template<typename Enable>
        struct static_version_impl {

        };

        template<uint8_t m, uint8_t h, uint16_t r, typename ... StaticRanges>
        struct static_version
                : public static_version_impl<typename std::enable_if<
                        steemit::type_traits::all_true<
                                std::is_same<
                                        steemit::type_traits::static_range<true>,
                                        StaticRanges
                                >::value...
                        >::value
                >::type> {
            static const version version_instance;
        };

        template<uint8_t MinorVersion, uint8_t HardforkVersion, uint16_t r, typename ... StaticRanges>
        const version static_version<MinorVersion, HardforkVersion, r, StaticRanges...>::version_instance = version(MinorVersion, HardforkVersion, r);
    }
} // steemit::protocol

namespace fc {
    class variant;

    void to_variant(const steemit::protocol::version &v, variant &var);

    void from_variant(const variant &var, steemit::protocol::version &v);

    void to_variant(const steemit::protocol::hardfork_version &hv, variant &var);

    void from_variant(const variant &var, steemit::protocol::hardfork_version &hv);
} // fc

#include <fc/reflect/reflect.hpp>

FC_REFLECT(steemit::protocol::version, (v_num))
FC_REFLECT_DERIVED(steemit::protocol::hardfork_version, (steemit::protocol::version),)

FC_REFLECT(steemit::protocol::hardfork_version_vote, (hf_version)(hf_time))
