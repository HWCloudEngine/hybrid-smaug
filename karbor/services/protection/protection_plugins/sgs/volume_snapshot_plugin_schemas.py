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
    "title": "SGS Snapshot Options",
    "type": "object",
    "properties": {
        "snapshot_name": {
            "type": "string",
            "title": "Snapshot Name",
            "description": "The name of the snapshot.",
            "default": None
        },
        "description": {
            "type": "string",
            "title": "Description",
            "description": "The description of the volume.",
            "default": None
        }
    },
    "required": []
}

RESTORE_SCHEMA = {
    "title": "SGS Snapshot Restore",
    "type": "object",
    "properties": {
        "volume_type": {
            "type": "string",
            "title": "Volume Type",
            "description": "The target volume type to restore.",
            "default": None
        },
        "restore_name": {
            "type": "string",
            "title": "Restore Name",
            "description": "The name of the restored volume.",
            "default": None
        },
        "description": {
            "type": "string",
            "title": "Restore Description",
            "description": "The description of the restored volume.",
            "default": None
        }
    }
}

SAVED_INFO_SCHEMA = {
    "title": "SGS Snapshot Saved Info",
    "type": "object",
    "properties": {
        "name": {
            "type": "string",
            "title": "Name",
            "description": "The name for this snapshot."
        },
        "is_remote": {
            "type": "boolean",
            "title": "Is remote snaphot",
            "description":
                "The type of the snapshot, "
        },
        "status": {
            "type": "string",
            "title": "Status",
            "description": "The snapshot status, such as available.",
            "enum": ['creating', 'available',
                                 'deleting', 'error'],
        },
        "volume_id": {
            "type": "string",
            "title": "Volume ID",
            "description":
                ("The ID of the volume "
                 "from which the snapshot was created.")
        },
    },
    "required": ["name", "status", "volume_id"]
}
