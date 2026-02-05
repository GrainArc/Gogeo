/*
Copyright (C) 2025 [GrainArc]

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published
by the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/
package Gogeo

import (
	"encoding/xml"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

var MainConfig PGConfig

type PGConfig struct {
	XMLName  xml.Name `xml:"config"`
	Dbname   string   `xml:"dbname"`
	Host     string   `xml:"host"`
	Port     string   `xml:"port"`
	Username string   `xml:"user"`
	Password string   `xml:"password"`
}

func init() {
	configDir, err := os.UserConfigDir()
	if err != nil {
		log.Fatal("无法获取用户配置目录:", err)
	}
	configdata := filepath.Join(configDir, "BoundlessMap", "config.xml")
	xmlFile, err := os.Open(configdata)
	if err != nil {
		fmt.Println("Error  opening  file:", err)
		return
	}
	defer xmlFile.Close()

	xmlDecoder := xml.NewDecoder(xmlFile)
	err = xmlDecoder.Decode(&MainConfig)
	if err != nil {
		fmt.Println("Error  decoding  XML:", err)
		return
	}

}
