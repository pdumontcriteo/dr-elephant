#
# Copyright 2016 LinkedIn Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

# --- !Ups
CREATE TABLE garmadon_yarn_app_heuristic_result (
  id                  INT(11)       NOT NULL AUTO_INCREMENT COMMENT 'The application heuristic result id',
  yarn_app_result_id  VARCHAR(50)   NOT NULL                COMMENT 'The application id',
  heuristic_class     VARCHAR(255)  NOT NULL                COMMENT 'Name of the JVM class that implements this heuristic',
  heuristic_name      VARCHAR(128)  NOT NULL                COMMENT 'The heuristic name',
  severity            TINYINT(2)    UNSIGNED NOT NULL       COMMENT 'The heuristic severity ranging from 0(LOW) to 4(CRITICAL)',
  score               MEDIUMINT(9)  UNSIGNED DEFAULT 0      COMMENT 'The heuristic score for the application. score = severity * number_of_tasks(map/reduce) where severity not in [0,1], otherwise score = 0',
  ready               BIT           NOT NULL DEFAULT 0      COMMENT 'Indicate if it is ready to be inserted in dr-elephant heuristics',
  read_times          TINYINT(2)    UNSIGNED DEFAULT 0      COMMENT 'Number of time the value has been read',

  PRIMARY KEY (id)
);

create index garmadon_yarn_app_heuristic_result_i1 on garmadon_yarn_app_heuristic_result (yarn_app_result_id, ready);

CREATE TABLE garmadon_yarn_app_heuristic_result_details (
  yarn_app_heuristic_result_id  INT(11) NOT NULL                  COMMENT 'The application heuristic result id',
  name                          VARCHAR(128) NOT NULL DEFAULT ''  COMMENT 'The analysis detail entry name/key',
  value                         VARCHAR(255) NOT NULL DEFAULT ''  COMMENT 'The analysis detail value corresponding to the name',
  details                       TEXT                              COMMENT 'More information on analysis details. e.g, stacktrace',

  PRIMARY KEY (yarn_app_heuristic_result_id,name),
  CONSTRAINT garmadon_yarn_app_heuristic_result_details_f1 FOREIGN KEY (yarn_app_heuristic_result_id) REFERENCES garmadon_yarn_app_heuristic_result (id)
);

# --- !Downs

SET FOREIGN_KEY_CHECKS=0;

DROP TABLE garmadon_yarn_app_heuristic_result;

DROP TABLE garmadon_yarn_app_heuristic_result_details

SET FOREIGN_KEY_CHECKS=1;

# --- Created by Ebean DDL
# To stop Ebean DDL generation, remove this comment and start using Evolutions
