#include "attr_cache.h"

namespace microsoft_azure {
    namespace storage {
        std::mutex attr_cache::s_attr_map_mutex;
        void attr_cache::add(const std::string &path, std::shared_ptr<attr_cache_entry> property)
        {
//            fprintf(stdout, "attr_cache add called with %s\n", path.c_str());
            std::lock_guard<std::mutex> lock(s_attr_map_mutex);
            attr_map[path] =  property;
        }
        void attr_cache::remove(const std::string &path)
        {
//            fprintf(stdout, "attr_cache remove called with %s\n", path.c_str());
            std::lock_guard<std::mutex> lock(s_attr_map_mutex);
            attr_map.erase(path);
        }

        std::shared_ptr<attr_cache_entry> attr_cache::get(const std::string &path)
        {
//            fprintf(stdout, "attr_cache get called with %s\n", path.c_str());
            std::lock_guard<std::mutex> lock(s_attr_map_mutex);
            auto iter = attr_map.find(path);
            if (iter != attr_map.end())
            {
//                std::stringstream ss;
//                ss << iter->second->access_time;
//                fprintf(stdout, "get succeeded.  name = %s, timestamp = %s\n", iter->second->name.c_str(), ss.str().c_str());
                return iter->second;
            }
            else
            {
//                fprintf(stdout, "get failed, returning nullptr\n");
                return nullptr;
            }
        }

/*        std::vector<std::shared_ptr<attr_cache_entry>> attr_cache::list_under(const std::string &path)
        {
            fprintf(stdout, "attr_cache list_under called with %s\n", path.c_str());
            std::lock_guard<std::mutex> lock(s_attr_map_mutex);
            auto lower_bound = attr_map.lower_bound(path);
            auto upper_bound = attr_map.upper_bound(path + "/");
            std::vector<std::shared_ptr<attr_cache_entry>> vec;

            for (;lower_bound != upper_bound; lower_bound++)
            {
                vec.push_back(lower_bound->second);
            }

            return vec;
        }
        */
    }
}