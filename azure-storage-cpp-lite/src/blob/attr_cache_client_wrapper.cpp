#include <string>
#include <errno.h>
#include "blob/blob_client.h"
#include "storage_errno.h"

namespace microsoft_azure {
    namespace storage {

std::vector<list_blobs_hierarchical_item> list_all_blobs(std::string container, std::string delimiter, std::string prefix, std::shared_ptr<blob_client_wrapper> blob_client_wrapper)
{
    static const int maxFailCount = 20;
    std::vector<list_blobs_hierarchical_item> results;

    std::string continuation;

    std::string prior;
    bool success = false;
    int failcount = 0;
    do
    {
        if (0)
        {
            fprintf(stdout, "About to call list_blobs_hierarchial.  Container = %s, delimiter = %s, continuation = %s, prefix = %s\n", container.c_str(), delimiter.c_str(), continuation.c_str(), prefix.c_str());
        }

        errno = 0;
        list_blobs_hierarchical_response response = blob_client_wrapper->list_blobs_hierarchical(container, delimiter, continuation, prefix);
        if (errno == 0)
        {
            success = true;
            failcount = 0;
            if (0)
            {
                fprintf(stdout, "results count = %s\n", to_str(response.blobs.size()).c_str());
                fprintf(stdout, "next_marker = %s\n", response.next_marker.c_str());
            }
            continuation = response.next_marker;
            if(response.blobs.size() > 0)
            {
                auto begin = response.blobs.begin();
                if(response.blobs[0].name == prior)
                {
                    std::advance(begin, 1);
                }
                results.insert(results.end(), begin, response.blobs.end());
                prior = response.blobs.back().name;
            }
        }
        else
        {
            failcount++;
            success = false;
            if (1)
            {
                fprintf(stdout, "list_blobs_hierarchical failed %d time with errno = %d\n", failcount, errno);
            }

        }
    } while (((continuation.size() > 0) || !success) && (failcount < maxFailCount));

    // errno will be set by list_blobs_hierarchial if the last call failed and we're out of retries.
    return results;
}

/*
 * Check if the direcotry is empty or not by checking if there is any blob with prefix exists in the specified container.
 *
 * return
 *   - D_NOTEXIST if there's nothing there (the directory does not exist)
 *   - D_EMPTY is there's exactly one blob, and it's the ".directory" blob
 *   - D_NOTEMPTY otherwise (the directory exists and is not empty.)
 */
int is_service_directory_empty(std::string container, std::string delimiter, std::string prefix, uint min_name_size, std::shared_ptr<blob_client_wrapper> blob_client_wrapper)
{
    std::string continuation;
    bool success = false;
    int failcount = 0;
    bool dirBlobFound = false;
    do
    {
        errno = 0;
        list_blobs_hierarchical_response response = blob_client_wrapper->list_blobs_hierarchical(container, delimiter, continuation, prefix);
        if (errno == 0)
        {
            success = true;
            failcount = 0;
            continuation = response.next_marker;
            if (response.blobs.size() > 1)
            {
                return D_NOTEMPTY;
            }
            if (response.blobs.size() == 1)
            {
                if ((!dirBlobFound) &&
                    (!response.blobs[0].is_directory) &&
                    (response.blobs[0].name.size() > min_name_size) &&
                    (0 == response.blobs[0].name.compare(response.blobs[0].name.size() - min_name_size, min_name_size, ".directory")))
                {
                    dirBlobFound = true;
                }
                else
                {
                    return D_NOTEMPTY;
                }
            }
        }
        else
        {
            success = false;
            failcount++; //TODO: use to set errno.
        }
    } while ((continuation.size() > 0 || !success) && failcount < 20);

    if (!success)
    {
    // errno will be set by list_blobs_hierarchial if the last call failed and we're out of retries.
        return -1;
    }
    
    return dirBlobFound ? D_EMPTY : D_NOTEXIST;
}

        /// <summary>
        /// Uploads the contents of a blob from a local file, file size need to be equal or smaller than 64MB.
        /// </summary>
        /// <param name="sourcePath">The source file path.</param>
        /// <param name="container">The container name.</param>
        /// <param name="blob">The blob name.</param>
        /// <param name="metadata">A <see cref="std::vector"> that respresents metadatas.</param>
        void attr_cache_client_wrapper::put_blob(const std::string &sourcePath, const std::string &container, const std::string blob, const std::vector<std::pair<std::string, std::string>> &metadata)
        {
            if (!m_use_attr_cache)
            {
                m_blob_client_wrapper->put_blob(sourcePath, container, blob, metadata);
            }
            else
            {
                // TODO:  actually use the return value from put_blob to store data in the cache
                m_attr_cache.remove(blob);
                m_blob_client_wrapper->put_blob(sourcePath, container, blob, metadata);
            }
        }

        /// <summary>
        /// Uploads the contents of a blob from a stream.
        /// </summary>
        /// <param name="container">The container name.</param>
        /// <param name="blob">The blob name.</param>
        /// <param name="is">The source stream.</param>
        /// <param name="metadata">A <see cref="std::vector"> that respresents metadatas.</param>
        void attr_cache_client_wrapper::upload_block_blob_from_stream(const std::string &container, const std::string blob, std::istream &is, const std::vector<std::pair<std::string, std::string>> &metadata)
        {
            if (!m_use_attr_cache)
            {
                m_blob_client_wrapper->upload_block_blob_from_stream(container, blob, is, metadata);
            }
            else
            {
                // TODO:  actually use the return value from put_blob to store data in the cache
                m_attr_cache.remove(blob);
                m_blob_client_wrapper->upload_block_blob_from_stream(container, blob, is, metadata);
            }
        }

        /// <summary>
        /// Uploads the contents of a blob from a local file.
        /// </summary>
        /// <param name="sourcePath">The source file path.</param>
        /// <param name="container">The container name.</param>
        /// <param name="blob">The blob name.</param>
        /// <param name="metadata">A <see cref="std::vector"> that respresents metadatas.</param>
        /// <param name="parallel">A size_t value indicates the maximum parallelism can be used in this request.</param>
        void attr_cache_client_wrapper::upload_file_to_blob(const std::string &sourcePath, const std::string &container, const std::string blob, const std::vector<std::pair<std::string, std::string>> &metadata, size_t parallel)
        {
            if (!m_use_attr_cache)
            {
                m_blob_client_wrapper->upload_file_to_blob(sourcePath, container, blob, metadata, parallel);
            }
            else
            {
                // TODO:  actually use the return value from put_blob to store data in the cache
                m_attr_cache.remove(blob);
                m_blob_client_wrapper->upload_file_to_blob(sourcePath, container, blob, metadata, parallel);
            }
        }

        /// <summary>
        /// Downloads the contents of a blob to a stream.
        /// </summary>
        /// <param name="container">The container name.</param>
        /// <param name="blob">The blob name.</param>
        /// <param name="offset">The offset at which to begin downloading the blob, in bytes.</param>
        /// <param name="size">The size of the data to download from the blob, in bytes.</param>
        /// <param name="os">The target stream.</param>
        void attr_cache_client_wrapper::download_blob_to_stream(const std::string &container, const std::string &blob, unsigned long long offset, unsigned long long size, std::ostream &os)
        {
            if (!m_use_attr_cache)
            {
                m_blob_client_wrapper->download_blob_to_stream(container, blob, offset, size, os);
            }
            else
            {
                // TODO:  actually use the return value from put_blob to store data in the cache
                m_blob_client_wrapper->download_blob_to_stream(container, blob, offset, size, os);
            }
        }

        /// <summary>
        /// Downloads the contents of a blob to a local file.
        /// </summary>
        /// <param name="container">The container name.</param>
        /// <param name="blob">The blob name.</param>
        /// <param name="offset">The offset at which to begin downloading the blob, in bytes.</param>
        /// <param name="size">The size of the data to download from the blob, in bytes.</param>
        /// <param name="destPath">The target file path.</param>
        /// <param name="parallel">A size_t value indicates the maximum parallelism can be used in this request.</param>
        void attr_cache_client_wrapper::download_blob_to_file(const std::string &container, const std::string &blob, const std::string &destPath, size_t parallel)
        {
            if (!m_use_attr_cache)
            {
                m_blob_client_wrapper->download_blob_to_file(container, blob, destPath, parallel);
            }
            else
            {
                // TODO:  actually use the return value from put_blob to store data in the cache
                m_blob_client_wrapper->download_blob_to_file(container, blob, destPath, parallel);
            }
        }

        /// <summary>
        /// Gets the property of a blob.
        /// </summary>
        /// <param name="container">The container name.</param>
        /// <param name="blob">The blob name.</param>
        blob_property attr_cache_client_wrapper::get_blob_property(const std::string &container, const std::string &blob)
        {
            if (!m_use_attr_cache)
            {
                return m_blob_client_wrapper->get_blob_property(container, blob);
            }

            std::shared_ptr<attr_cache_entry> blobEntry = m_attr_cache.get(blob);
            time_t curTime = time(NULL);
            if (blobEntry == nullptr || !(curTime - blobEntry->access_time < m_attr_cache_timeout_in_seconds))
            {
/*                std::stringstream ss;
                ss << curTime;
                fprintf(stdout, "attr_cache null or timeout for %s get props, curtime = %s, timeout = %i", blob.c_str(), ss.str().c_str(), m_attr_cache_timeout_in_seconds);
                if (blobEntry != nullptr)
                {
                    time_t diffTime = curTime - blobEntry->access_time;
                    std::stringstream css;
                    css << diffTime;
                    fprintf(stdout, ", difftime = %s\n", css.str().c_str());
                }
                else
                {
                    fprintf(stdout, "\n");
                }

                fprintf(stdout, "adding %s to attr_cache from get_blob_property operation\n", blob.c_str());
                */
                errno = 0
                blob_property props = m_blob_client_wrapper->get_blob_property(container, blob);
                if (errno == 0)
                {
                    attr_cache_entry entry(props, time(NULL), blob, false /* is_directory */);
                    std::shared_ptr<attr_cache_entry> entryptr = std::make_shared<attr_cache_entry>(entry);
                    m_attr_cache.add(blob, entryptr);
                }

                return props;
            }

//            fprintf(stdout, "%s props found in attr_cache\n", blob.c_str());
            return blobEntry->properties;
        }

        /// <summary>
        /// Examines the existance of a blob.
        /// </summary>
        /// <param name="container">The container name.</param>
        /// <param name="blob">The blob name.</param>
        /// <returns>Return true if the blob does exist, otherwise, return false.</returns>
        bool attr_cache_client_wrapper::blob_exists(const std::string &container, const std::string &blob)
        {
            if (!m_use_attr_cache)
            {
                return m_blob_client_wrapper->blob_exists(container, blob);
            }

            std::shared_ptr<attr_cache_entry> blobEntry = m_attr_cache.get(blob);
            if (blobEntry == nullptr || !(time(NULL) - blobEntry->access_time < m_attr_cache_timeout_in_seconds))
            {
                // TODO: update the attr_cache with this information
                return m_blob_client_wrapper->blob_exists(container, blob);
            }
            return true;
        }

        /// <summary>
        /// Deletes a blob.
        /// </summary>
        /// <param name="container">The container name.</param>
        /// <param name="blob">The blob name.</param>
        void attr_cache_client_wrapper::delete_blob(const std::string &container, const std::string &blob)
        {
            if (!m_use_attr_cache)
            {
                m_blob_client_wrapper->delete_blob(container, blob);
            }
            else
            {
                m_attr_cache.remove(blob);
                m_blob_client_wrapper->delete_blob(container, blob);
            }
        }

        std::vector<list_blobs_hierarchical_item> add_to_attr_cache(std::vector<list_blobs_hierarchical_item> items, attr_cache& attr_cache)
        {
            for (auto it = items.begin(); it != items.end(); it++)
            {
                if (!it->is_directory)
                {
//                    fprintf(stdout, "adding %s to attr_cache from list operation\n", it->name.c_str());
                    blob_property properties;
                    properties.size = it->content_length;
                    properties.etag = it->etag;
                    properties.metadata = it->metadata;
                    attr_cache_entry entry(properties, time(NULL), it->name, it->is_directory);
                    std::shared_ptr<attr_cache_entry> entryptr = std::make_shared<attr_cache_entry>(entry);
                    attr_cache.add(it->name, entryptr);
                }
            }
            return items;
        }

        /// <summary>
        /// List blobs in segments.
        /// </summary>
        /// <param name="container">The container name.</param>
        /// <param name="delimiter">The delimiter used to designate the virtual directories.</param>
        /// <param name="continuation_token">A continuation token returned by a previous listing operation.</param>
        /// <param name="prefix">The blob name prefix.</param>
        std::vector<list_blobs_hierarchical_item> attr_cache_client_wrapper::list_all_blobs_hierarchical(const std::string &container, const std::string &delimiter, const std::string &prefix)
        {
//            fprintf(stdout, "list called\n");
            if (!m_use_attr_cache)
            {
//                fprintf(stdout, "attr_cache_disabled\n");
                return list_all_blobs(container, delimiter, prefix, m_blob_client_wrapper);
            }
            return add_to_attr_cache(list_all_blobs(container, delimiter, prefix, m_blob_client_wrapper), m_attr_cache);

/*
            std::string dirstring = prefix + ".directory";
            fprintf(stdout, "checking %s for listing operation in cache\n", dirstring.c_str());
            std::shared_ptr<attr_cache_entry> dirEntry = m_attr_cache.get(dirstring);
            time_t curTime = time(NULL);
            if (dirEntry == nullptr || !(curTime - dirEntry->access_time < m_attr_cache_timeout_in_seconds))
            {
                std::stringstream ss;
                ss << curTime;
                fprintf(stdout, "attr_cache null or timeout for list all under %s, curtime = %s", prefix.c_str(), ss.str().c_str());
                if (dirEntry != nullptr)
                {
                    time_t diffTime = curTime - dirEntry->access_time;
                    std::stringstream css;
                    css << diffTime;
                    fprintf(stdout, ", difftime = %s\n", css.str().c_str());
                }
                else
                {
                    fprintf(stdout, "\n");
                }
                return add_to_attr_cache(list_all_blobs(container, delimiter, prefix, m_blob_client_wrapper), m_attr_cache);
            }

            fprintf(stdout, "attr_cache valid for list all under %s\n", prefix.c_str());
            std::vector<std::shared_ptr<attr_cache_entry>> entries = m_attr_cache.list_under(prefix);
            std::vector<list_blobs_hierarchical_item> results(entries.size());
            for (auto it = entries.begin();it != entries.end(); it++)
            {
                list_blobs_hierarchical_item result;
                result.name = (*it)->name;
                result.snapshot = (*it)->snapshot;
                result.last_modified = (*it)->last_modified;
                result.etag = (*it)->properties.etag;
                result.content_length = (*it)->properties.size;
                result.content_encoding = (*it)->properties.content_encoding;
                result.content_type = (*it)->properties.content_type;
                result.content_md5 = (*it)->properties.content_md5;
                result.content_language = (*it)->properties.content_language;
                result.cache_control = (*it)->properties.cache_control;
                result.status = (*it)->status;
                result.state = (*it)->state;
                result.duration = (*it)->duration;
                result.metadata = (*it)->properties.metadata;
                result.is_directory = (*it)->is_directory;
                results.push_back(result);
            }
            return results;
            */

        }

        int attr_cache_client_wrapper::is_directory_empty(std::string container, std::string delimiter, std::string prefix, int min_name_size)
        {
//            if (!m_use_attr_cache)
//            {
                return is_service_directory_empty(container, delimiter, prefix, min_name_size, m_blob_client_wrapper);
  //          }
    //        else
      //      {
        //        return 0;
          //  }
        }
}}