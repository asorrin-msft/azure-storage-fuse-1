#include <mutex>
#include "common.h"
#include "get_blob_property_request_base.h"


namespace microsoft_azure {
    namespace storage {
        class attr_cache_entry  {
        public:
            blob_property properties;
            time_t access_time;
            std::string name;
            std::string snapshot;
            std::string last_modified;
            lease_status status;
            lease_state state;
            lease_duration duration;
            bool is_directory;
            std::string content_disposition;
            std::string copy_status;

            attr_cache_entry() : properties(blob_property(true))
            {

            }

            attr_cache_entry(blob_property properties, time_t access_time, std::string name, bool is_directory) : properties(properties), access_time(access_time), name(name), is_directory(is_directory)
            {

            }

        };

        class attr_cache {
        public:
            void add(const std::string &path, std::shared_ptr<attr_cache_entry> property);
            std::shared_ptr<attr_cache_entry> get(const std::string &path);
            void remove(const std::string &path);
            std::vector<std::shared_ptr<attr_cache_entry>> list_under(const std::string &path);

            attr_cache()
            {

            }

        private:
            std::map<const std::string, std::shared_ptr<attr_cache_entry>> attr_map;
            static std::mutex s_attr_cache_mutex;
        };
    }
}