#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

OPTIONS_SCHEMA = {
    "title": "SGS Replication Options",
    "type": "object",
    "properties": {
        "replication_name": {
            "type": "string",
            "title": "Replication Name",
            "description": "The name of the replication.",
            "default": None
        },
        "replicate_name": {
            "type": "string",
            "title": "Replicate Name",
            "description": "The name of the replicate volume.",
            "default": None
        },
        "volume_type": {
            "type": "string",
            "title": "Volume Type",
            "description": "The volume type of the replicate volume.",
            "default": None
        },
        "replicate_volume": {
            "type": "string",
            "title": "Replicate volume",
            "description": "The id of the replicate volume.",
            "default": None
        }
    },
    "required": ["replication_name"]
}

RESTORE_SCHEMA = {
    "title": "SGS Replication Restore",
    "type": "object",
    "properties": {
        "is_force": {
            "type": "boolean",
            "title": "Is force failover and reverse",
            "description": "Whether force failover and reverse "
                           "replication or not",
            "default": False
        },
    }
}

SAVED_INFO_SCHEMA = {
    "title": "SGS Snapshot Saved Info",
    "type": "object",
    "properties": {
        "replicate_volume": {
            "type": "string",
            "title": "Replicate volume",
            "description": "The id of replicate volume"
        },
        "replication_id": {
            "type": "string",
            "title": "Replication id",
            "description": "The id of replication"
        },
    },
    "required": ["replicate_volume", "replication_id"]
}
