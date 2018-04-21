#include <golos/plugins/mongo_db/mongo_db_writer.hpp>
#include <golos/plugins/mongo_db/mongo_db_operations.hpp>
#include <golos/plugins/chain/plugin.hpp>
#include <golos/protocol/operations.hpp>

#include <fc/log/logger.hpp>
#include <appbase/application.hpp>

#include <mongocxx/exception/exception.hpp>
#include <bsoncxx/array/element.hpp>
#include <bsoncxx/builder/stream/array.hpp>


namespace golos {
namespace plugins {
namespace mongo_db {

    using namespace bsoncxx::types;
    using namespace bsoncxx::builder;
    using bsoncxx::builder::stream::array;
    using bsoncxx::builder::basic::kvp;

    const std::string mongo_db_writer::blocks = "Blocks";
    const std::string mongo_db_writer::transactions = "Transactions";
    const std::string mongo_db_writer::operations = "Operations";

    mongo_db_writer::mongo_db_writer() :
            _db(appbase::app().get_plugin<golos::plugins::chain::plugin>().db()) {
    }

    mongo_db_writer::~mongo_db_writer() {
    }

    bool mongo_db_writer::initialize(const std::string& uri_str, const bool write_raw, const std::vector<std::string>& op) {
        try {
            uri = mongocxx::uri {uri_str};
            mongo_conn = mongocxx::client {uri};
            db_name = uri.database().empty() ? "Golos" : uri.database();
            mongo_database = mongo_conn[db_name];
            bulk_opts.ordered(false);
            write_raw_blocks = write_raw;
            write_operations = op;

            ilog("MongoDB writer initialized.");

            return true;
        }
        catch (mongocxx::exception & ex) {
            ilog("Exception in MongoDB initialize: ${p}", ("p", ex.what()));
            return false;
        }
        catch (...) {
            ilog("Unknown exception in MongoDB writer");
            return false;
        }
    }

    void mongo_db_writer::on_block(const signed_block& block) {

        try {

            ilog("MongoDB mongo_db_writer::on_block processed blocks: ${e}", ("e", processed_blocks));

            _blocks[block.block_num()] = block;

            // Update last irreversible block number
            last_irreversible_block_num = _db.last_non_undoable_block_num();
            if (last_irreversible_block_num >= _blocks.begin()->first) {

                // Write all the blocks that has num less then last irreversible block
                while (!_blocks.empty() && _blocks.begin()->first <= last_irreversible_block_num) {
                    auto head_iter = _blocks.begin();

                    try {
                        if (write_raw_blocks) {
                            write_raw_block(head_iter->second);
                        }

                        write_block_operations(head_iter->second);
                    }
                    catch (...) {
                        // If some block causes any problems lets remove it from buffer and move on
                        _blocks.erase(head_iter);
                        throw;
                    }
                    _blocks.erase(head_iter);
                }
                write_data();
            }

            ++processed_blocks;
        }
        catch (...) {
            ilog("Unknown exception in MongoDB");
        }
    }

    void mongo_db_writer::write_raw_block(const signed_block& block) {

        //ilog("mongo_db_writer::write_raw_block ${e}", ("e", block.block_num()));

        document block_doc;
        format_block_info(block, block_doc);

        array transactions_array;

        // Now write every transaction from Block
        for (const auto& tran : block.transactions) {

            document tran_doc;
            format_transaction_info(tran, tran_doc);

            if (!tran.operations.empty()) {

                array operations_array;

                // Write every operation in transaction
                for (const auto& op : tran.operations) {

                    try {
                        operation_writer op_writer;
                        op.visit(op_writer);

                        std::vector<document_ptr> docs = op_writer.get_documents();

                        for (document_ptr doc : docs) {
                            operations_array << *doc;
                        }
                    }
                    catch (fc::exception& ex) {
                        ilog("MongoDB write_raw_block fc::exception ${e}", ("e", ex.what()));
                    }
                    catch (mongocxx::exception& ex) {
                        ilog("Mongodb write_raw_block Mongo exception ${e}", ("e", ex.what()));
                    }
                    catch (std::exception& ex) {
                        ilog("Mongodb write_raw_block std exception ${e}", ("e", ex.what()));
                    }
                    catch (...) {
                        ilog("Mongodb write_raw_block unknown exception ");
                    }
                }

                tran_doc << operations << operations_array;
            }

            transactions_array << tran_doc;
        }

        block_doc << transactions << transactions_array;

        if (_formatted_blocks.find(blocks) == _formatted_blocks.end()) {
            std::shared_ptr<mongocxx::bulk_write> write(new mongocxx::bulk_write(bulk_opts));
            _formatted_blocks[blocks] = write;
        }

        mongocxx::model::insert_one insert_msg{block_doc.view()};
        _formatted_blocks[blocks]->append(insert_msg);
    }

    void mongo_db_writer::write_block_operations(const signed_block& block) {

        //ilog("mongo_db_writer::write_block_operations ${e}", ("e", block.transactions.size()));

        // Now write every transaction from Block
        for (const auto& tran : block.transactions) {

            for (const auto& op : tran.operations) {

                ilog("Extracting operation");
                operation_writer op_writer;
                op.visit(op_writer);

                ilog("Extracting operation name");
                operation_name op_name;
                op.visit(op_name);

                ilog("Operation name ${e}", ("e", op_name.get_result()));

                auto iter = std::find(write_operations.begin(), write_operations.end(), op_name.get_result());
                if (!write_operations.empty() && iter == write_operations.end()) {
                    continue;
                }

                std::vector<document_ptr> doc_arr = op_writer.get_documents();
                for (document_ptr doc : doc_arr) {
                    format_transaction_info(tran, *doc);
                    format_block_info(block, *doc);

                    if (_formatted_blocks.find(op_name.get_result()) == _formatted_blocks.end()) {
                        std::shared_ptr<mongocxx::bulk_write> write(new mongocxx::bulk_write(bulk_opts));
                        _formatted_blocks[op_name.get_result()] = write;
                    }

                    mongocxx::model::insert_one insert_msg{doc->view()};
                    _formatted_blocks[op_name.get_result()]->append(insert_msg);

                }
            }
        }
    }

    void mongo_db_writer::format_block_info(const signed_block& block, document& doc) {

        ilog("mongo_db_writer::format_block_info");

        doc << "block_num"              << std::to_string(block.block_num())
            << "block_id"               << block.id().str()
            << "block_prev_block_id"    << block.previous.str()
            << "block_timestamp"        << block.timestamp
            << "block_witness"          << block.witness
            << "block_created_at"       << fc::time_point::now();
    }

    void mongo_db_writer::format_transaction_info(const signed_transaction& tran, document& doc) {

        ilog("mongo_db_writer::format_transaction_info");

        doc << "transaction_id"             << tran.id().str()
            << "transaction_ref_block_num"  << std::to_string(tran.ref_block_num)
            << "transaction_expiration"     << tran.expiration;
    }

    void mongo_db_writer::write_data() {

        ilog("mongo_db_writer::write_data");

        auto iter = _formatted_blocks.begin();
        for (; iter != _formatted_blocks.end(); ++iter) {

            auto oper = *iter;
            try {
                const std::string& collection_name = oper.first;
                mongocxx::collection _collection = get_active_collection(collection_name);

                bulk_ptr bulkp = oper.second;
                if (!_collection.bulk_write(*bulkp)) {
                    ilog("Failed to write blocks to Mongo DB");
                }
            }
            catch (...) {
                ilog("Unknown exception while writing blocks to mongo");
                // If we got some errors writing block into mongo just skip this block and move on
                _formatted_blocks.erase(iter);
                throw;
            }
        }
        _formatted_blocks.clear();
    }

    mongocxx::collection mongo_db_writer::get_active_collection(const std::string& coll_type) {

        if (active_collections.find(coll_type) == active_collections.end()) {
            std::string coll_name = coll_type + std::string("_1");
            mongocxx::collection coll = mongo_database[coll_name];
            active_collections[coll_type] = coll;
            return coll;
        }
        else {
            mongocxx::collection coll = active_collections[coll_type];
            std::string coll_name = coll.name().to_string();

            const int64_t coll_size = coll.count(document{} << bsoncxx::builder::stream::finalize);

            if (coll_size > max_collection_size) {

                std::string new_coll_name;

                std::string::iterator iter = std::find(coll_name.begin(), coll_name.end(), '_');
                if (iter != coll_name.end()) {
                    // Always should be here
                    std::string coll_num_str = std::string(iter + 1, coll_name.end());
                    std::istringstream ss(coll_num_str);
                    int coll_num = 0;
                    ss >> coll_num;
                    if (coll_num != 0) {
                        new_coll_name = coll_type + std::string("_") + std::to_string(coll_num + 1);
                    }
                }
                mongocxx::collection coll = mongo_database[new_coll_name];
                active_collections[coll_type] = coll;
                return coll;
            }
            else {
                return active_collections[coll_type];
            }
        }
    }
}}}
