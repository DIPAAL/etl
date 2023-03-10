CREATE SCHEMA IF NOT EXISTS staging;

-- Create map table for 'Maritime Identification Digit'
-- Mapping found on: https://www.itu.int/en/ITU-R/terrestrial/fmd/Pages/mid.aspx
-- Ship stations found on: https://help.marinetraffic.com/hc/en-us/articles/360018392858-How-does-MarineTraffic-identify-a-vessel-s-country-and-flag-
CREATE TABLE IF NOT EXISTS mid_map (
    mid SMALLINT NOT NULL,
    region_flag TEXT NOT NULL,
    flag_state TEXT NOT NULL
);

INSERT INTO mid_map (mid, region_flag, flag_state) VALUES
    (201, 'europe', 'Albania (Republic of)'),
    (202, 'europe', 'Andorra (Principality of)'),
    (203, 'europe', 'Austria'),
    (204, 'europe', 'Portugal - Azores'),
    (205, 'europe', 'Belgium'),
    (206, 'europe', 'Belarus (Republic of)'),
    (207, 'europe', 'Bulgaria (Republic of)'),
    (208, 'europe', 'Vatican City State'),
    (209, 'europe', 'Cyprus (Republic of)'),
    (210, 'europe', 'Cyprus (Republic of)'),
    (211, 'europe', 'Germany (Federal Republic of)'),
    (212, 'europe', 'Cyprus (Republic of)'),
    (213, 'europe', 'Georgia'),
    (214, 'europe', 'Moldova (Republic of)'),
    (215, 'europe', 'Malta'),
    (216, 'europe', 'Armenia (Republic of)'),
    (218, 'europe', 'Germany (Federal Republic of)'),
    (219, 'europe', 'Denmark'),
    (220, 'europe', 'Denmark'),
    (224, 'europe', 'Spain'),
    (225, 'europe', 'Spain'),
    (226, 'europe', 'France'),
    (227, 'europe', 'France'),
    (228, 'europe', 'France'),
    (229, 'europe', 'Malta'),
    (230, 'europe', 'Finland'),
    (231, 'europe', 'Denmark - Faroe Islands'),
    (232, 'europe', 'United Kingdom of Great Britain and Northern Ireland'),
    (233, 'europe', 'United Kingdom of Great Britain and Northern Ireland'),
    (234, 'europe', 'United Kingdom of Great Britain and Northern Ireland'),
    (235, 'europe', 'United Kingdom of Great Britain and Northern Ireland'),
    (236, 'europe', 'United Kingdom of Great Britain and Northern Ireland - Gibraltar'),
    (237, 'europe', 'Greece'),
    (238, 'europe', 'Croatia (Republic of)'),
    (239, 'europe', 'Greece'),
    (240, 'europe', 'Greece'),
    (241, 'europe', 'Greece'),
    (242, 'europe', 'Morocco (Kingdom of)'),
    (243, 'europe', 'Hungary'),
    (244, 'europe', 'Netherlands (Kingdom of the)'),
    (245, 'europe', 'Netherlands (Kingdom of the)'),
    (246, 'europe', 'Netherlands (Kingdom of the)'),
    (247, 'europe', 'Italy'),
    (248, 'europe', 'Malta'),
    (249, 'europe', 'Malta'),
    (250, 'europe', 'Ireland'),
    (251, 'europe', 'Iceland'),
    (252, 'europe', 'Liechtenstein (Principality of)'),
    (253, 'europe', 'Luxembourg'),
    (254, 'europe', 'Monaco (Principality of)'),
    (255, 'europe', 'Portugal - Madeira'),
    (256, 'europe', 'Malta'),
    (257, 'europe', 'Norway'),
    (258, 'europe', 'Norway'),
    (259, 'europe', 'Norway'),
    (261, 'europe', 'Poland (Republic of)'),
    (262, 'europe', 'Montenegro'),
    (263, 'europe', 'Portugal'),
    (264, 'europe', 'Romania'),
    (265, 'europe', 'Sweden'),
    (266, 'europe', 'Sweden'),
    (267, 'europe', 'Slovak Republic'),
    (268, 'europe', 'San Marino (Republic of)'),
    (269, 'europe', 'Switzerland (Confederation of)'),
    (270, 'europe', 'Czech Republic'),
    (271, 'europe', 'Republic of Türkiye'),
    (272, 'europe', 'Ukraine'),
    (273, 'europe', 'Russian Federation'),
    (274, 'europe', 'North Macedonia (Republic of)'),
    (275, 'europe', 'Latvia (Republic of)'),
    (276, 'europe', 'Estonia (Republic of)'),
    (277, 'europe', 'Lithuania (Republic of)'),
    (278, 'europe', 'Slovenia (Republic of)'),
    (279, 'europe', 'Serbia (Republic of)'),
    (301, 'north america, central america and caribbean', 'United Kingdom of Great Britain and Northern Ireland - Anguilla'),
    (303, 'north america, central america and caribbean', 'United States of America - Alaska (State of)'),
    (304, 'north america, central america and caribbean', 'Antigua and Barbuda'),
    (305, 'north america, central america and caribbean', 'Antigua and Barbuda'),
    (306, 'north america, central america and caribbean', 'Netherlands (Kingdom of the) - Bonaire, Sint Eustatius and Saba'),
    (306, 'north america, central america and caribbean', 'Netherlands (Kingdom of the) - Curaçao'),
    (306, 'north america, central america and caribbean', 'Netherlands (Kingdom of the) - Sint Maarten (Dutch part)'),
    (307, 'north america, central america and caribbean', 'Netherlands (Kingdom of the) - Aruba'),
    (308, 'north america, central america and caribbean', 'Bahamas (Commonwealth of the)'),
    (309, 'north america, central america and caribbean', 'Bahamas (Commonwealth of the)'),
    (310, 'north america, central america and caribbean', 'United Kingdom of Great Britain and Northern Ireland - Bermuda'),
    (311, 'north america, central america and caribbean', 'Bahamas (Commonwealth of the)'),
    (312, 'north america, central america and caribbean', 'Belize'),
    (314, 'north america, central america and caribbean', 'Barbados'),
    (316, 'north america, central america and caribbean', 'Canada'),
    (319, 'north america, central america and caribbean', 'United Kingdom of Great Britain and Northern Ireland - Cayman Islands'),
    (321, 'north america, central america and caribbean', 'Costa Rica'),
    (323, 'north america, central america and caribbean', 'Cuba'),
    (325, 'north america, central america and caribbean', 'Dominica (Commonwealth of)'),
    (327, 'north america, central america and caribbean', 'Dominican Republic'),
    (329, 'north america, central america and caribbean', 'France - Guadeloupe (French Department of)'),
    (330, 'north america, central america and caribbean', 'Grenada'),
    (331, 'north america, central america and caribbean', 'Denmark - Greenland'),
    (332, 'north america, central america and caribbean', 'Guatemala (Republic of)'),
    (334, 'north america, central america and caribbean', 'Honduras (Republic of)'),
    (336, 'north america, central america and caribbean', 'Haiti (Republic of)'),
    (338, 'north america, central america and caribbean', 'United States of America'),
    (339, 'north america, central america and caribbean', 'Jamaica'),
    (341, 'north america, central america and caribbean', 'Saint Kitts and Nevis (Federation of)'),
    (343, 'north america, central america and caribbean', 'Saint Lucia'),
    (345, 'north america, central america and caribbean', 'Mexico'),
    (347, 'north america, central america and caribbean', 'France - Martinique (French Department of)'),
    (348, 'north america, central america and caribbean', 'United Kingdom of Great Britain and Northern Ireland - Montserrat'),
    (350, 'north america, central america and caribbean', 'Nicaragua'),
    (351, 'north america, central america and caribbean', 'Panama (Republic of)'),
    (352, 'north america, central america and caribbean', 'Panama (Republic of)'),
    (353, 'north america, central america and caribbean', 'Panama (Republic of)'),
    (354, 'north america, central america and caribbean', 'Panama (Republic of)'),
    (355, 'north america, central america and caribbean', 'Panama (Republic of)'),
    (356, 'north america, central america and caribbean', 'Panama (Republic of)'),
    (357, 'north america, central america and caribbean', 'Panama (Republic of)'),
    (358, 'north america, central america and caribbean', 'United States of America - Puerto Rico'),
    (359, 'north america, central america and caribbean', 'El Salvador (Republic of)'),
    (361, 'north america, central america and caribbean', 'France - Saint Pierre and Miquelon (Territorial Collectivity of)'),
    (362, 'north america, central america and caribbean', 'Trinidad and Tobago'),
    (364, 'north america, central america and caribbean', 'United Kingdom of Great Britain and Northern Ireland - Turks and Caicos Islands'),
    (366, 'north america, central america and caribbean', 'United States of America'),
    (367, 'north america, central america and caribbean', 'United States of America'),
    (368, 'north america, central america and caribbean', 'United States of America'),
    (369, 'north america, central america and caribbean', 'United States of America'),
    (370, 'north america, central america and caribbean', 'Panama (Republic of)'),
    (371, 'north america, central america and caribbean', 'Panama (Republic of)'),
    (372, 'north america, central america and caribbean', 'Panama (Republic of)'),
    (373, 'north america, central america and caribbean', 'Panama (Republic of)'),
    (374, 'north america, central america and caribbean', 'Panama (Republic of)'),
    (375, 'north america, central america and caribbean', 'Saint Vincent and the Grenadines'),
    (376, 'north america, central america and caribbean', 'Saint Vincent and the Grenadines'),
    (377, 'north america, central america and caribbean', 'Saint Vincent and the Grenadines'),
    (378, 'north america, central america and caribbean', 'United Kingdom of Great Britain and Northern Ireland - British Virgin Islands'),
    (379, 'north america, central america and caribbean', 'United States of America - United States Virgin Islands'),
    (401, 'asia', 'Afghanistan'),
    (403, 'asia', 'Saudi Arabia (Kingdom of)'),
    (405, 'asia', 'Bangladesh (Peoples Republic of)'),
    (408, 'asia', 'Bahrain (Kingdom of)'),
    (410, 'asia', 'Bhutan (Kingdom of)'),
    (412, 'asia', 'China (Peoples Republic of)'),
    (413, 'asia', 'China (Peoples Republic of)'),
    (414, 'asia', 'China (Peoples Republic of)'),
    (416, 'asia', 'China (Peoples Republic of) - Taiwan (Province of China)'),
    (417, 'asia', 'Sri Lanka (Democratic Socialist Republic of)'),
    (419, 'asia', 'India (Republic of)'),
    (422, 'asia', 'Iran (Islamic Republic of)'),
    (423, 'asia', 'Azerbaijan (Republic of)'),
    (425, 'asia', 'Iraq (Republic of)'),
    (428, 'asia', 'Israel (State of)'),
    (431, 'asia', 'Japan'),
    (432, 'asia', 'Japan'),
    (434, 'asia', 'Turkmenistan'),
    (436, 'asia', 'Kazakhstan (Republic of)'),
    (437, 'asia', 'Uzbekistan (Republic of)'),
    (438, 'asia', 'Jordan (Hashemite Kingdom of)'),
    (440, 'asia', 'Korea (Republic of)'),
    (441, 'asia', 'Korea (Republic of)'),
    (443, 'asia', 'State of Palestine (In accordance with Resolution 99 Rev. Dubai, 2018)'),
    (445, 'asia', 'Democratic Peoples Republic of Korea'),
    (447, 'asia', 'Kuwait (State of)'),
    (450, 'asia', 'Lebanon'),
    (451, 'asia', 'Kyrgyz Republic'),
    (453, 'asia', 'China (Peoples Republic of) - Macao (Special Administrative Region of China)'),
    (455, 'asia', 'Maldives (Republic of)'),
    (457, 'asia', 'Mongolia'),
    (459, 'asia', 'Nepal (Federal Democratic Republic of)'),
    (461, 'asia', 'Oman (Sultanate of)'),
    (463, 'asia', 'Pakistan (Islamic Republic of)'),
    (466, 'asia', 'Qatar (State of)'),
    (468, 'asia', 'Syrian Arab Republic'),
    (470, 'asia', 'United Arab Emirates'),
    (471, 'asia', 'United Arab Emirates'),
    (472, 'asia', 'Tajikistan (Republic of)'),
    (473, 'asia', 'Yemen (Republic of)'),
    (475, 'asia', 'Yemen (Republic of)'),
    (477, 'asia', 'China (Peoples Republic of) - Hong Kong (Special Administrative Region of China)'),
    (478, 'asia', 'Bosnia and Herzegovina'),
    (501, 'oceania', 'France - Adelie Land'),
    (503, 'oceania', 'Australia'),
    (506, 'oceania', 'Myanmar (Union of)'),
    (508, 'oceania', 'Brunei Darussalam'),
    (510, 'oceania', 'Micronesia (Federated States of)'),
    (511, 'oceania', 'Palau (Republic of)'),
    (512, 'oceania', 'New Zealand'),
    (514, 'oceania', 'Cambodia (Kingdom of)'),
    (515, 'oceania', 'Cambodia (Kingdom of)'),
    (516, 'oceania', 'Australia - Christmas Island (Indian Ocean)'),
    (518, 'oceania', 'New Zealand - Cook Islands'),
    (520, 'oceania', 'Fiji (Republic of)'),
    (523, 'oceania', 'Australia - Cocos (Keeling) Islands'),
    (525, 'oceania', 'Indonesia (Republic of)'),
    (529, 'oceania', 'Kiribati (Republic of)'),
    (531, 'oceania', 'Lao Peoples Democratic Republic'),
    (533, 'oceania', 'Malaysia'),
    (536, 'oceania', 'United States of America - Northern Mariana Islands (Commonwealth of the)'),
    (538, 'oceania', 'Marshall Islands (Republic of the)'),
    (540, 'oceania', 'France - New Caledonia'),
    (542, 'oceania', 'New Zealand - Niue'),
    (544, 'oceania', 'Nauru (Republic of)'),
    (546, 'oceania', 'France - French Polynesia'),
    (548, 'oceania', 'Philippines (Republic of the)'),
    (550, 'oceania', 'Timor-Leste (Democratic Republic of)'),
    (553, 'oceania', 'Papua New Guinea'),
    (555, 'oceania', 'United Kingdom of Great Britain and Northern Ireland - Pitcairn Island'),
    (557, 'oceania', 'Solomon Islands'),
    (559, 'oceania', 'United States of America - American Samoa'),
    (561, 'oceania', 'Samoa (Independent State of)'),
    (563, 'oceania', 'Singapore (Republic of)'),
    (564, 'oceania', 'Singapore (Republic of)'),
    (565, 'oceania', 'Singapore (Republic of)'),
    (566, 'oceania', 'Singapore (Republic of)'),
    (567, 'oceania', 'Thailand'),
    (570, 'oceania', 'Tonga (Kingdom of)'),
    (572, 'oceania', 'Tuvalu'),
    (574, 'oceania', 'Viet Nam (Socialist Republic of)'),
    (576, 'oceania', 'Vanuatu (Republic of)'),
    (577, 'oceania', 'Vanuatu (Republic of)'),
    (578, 'oceania', 'France - Wallis and Futuna Islands'),
    (601, 'africa', 'South Africa (Republic of)'),
    (603, 'africa', 'Angola (Republic of)'),
    (605, 'africa', 'Algeria (Peoples Democratic Republic of)'),
    (607, 'africa', 'France - Saint Paul and Amsterdam Islands'),
    (608, 'africa', 'United Kingdom of Great Britain and Northern Ireland - Ascension Island'),
    (609, 'africa', 'Burundi (Republic of)'),
    (610, 'africa', 'Benin (Republic of)'),
    (611, 'africa', 'Botswana (Republic of)'),
    (612, 'africa', 'Central African Republic'),
    (613, 'africa', 'Cameroon (Republic of)'),
    (615, 'africa', 'Congo (Republic of the)'),
    (616, 'africa', 'Comoros (Union of the)'),
    (617, 'africa', 'Cabo Verde (Republic of)'),
    (618, 'africa', 'France - Crozet Archipelago'),
    (619, 'africa', 'Côte d Ivoire (Republic of)'),
    (620, 'africa', 'Comoros (Union of the)'),
    (621, 'africa', 'Djibouti (Republic of)'),
    (622, 'africa', 'Egypt (Arab Republic of)'),
    (624, 'africa', 'Ethiopia (Federal Democratic Republic of)'),
    (625, 'africa', 'Eritrea'),
    (626, 'africa', 'Gabonese Republic'),
    (627, 'africa', 'Ghana'),
    (629, 'africa', 'Gambia (Republic of the)'),
    (630, 'africa', 'Guinea-Bissau (Republic of)'),
    (631, 'africa', 'Equatorial Guinea (Republic of)'),
    (632, 'africa', 'Guinea (Republic of)'),
    (633, 'africa', 'Burkina Faso'),
    (634, 'africa', 'Kenya (Republic of)'),
    (635, 'africa', 'France - Kerguelen Islands'),
    (636, 'africa', 'Liberia (Republic of)'),
    (637, 'africa', 'Liberia (Republic of)'),
    (638, 'africa', 'South Sudan (Republic of)'),
    (642, 'africa', 'Libya (State of)'),
    (644, 'africa', 'Lesotho (Kingdom of)'),
    (645, 'africa', 'Mauritius (Republic of)'),
    (647, 'africa', 'Madagascar (Republic of)'),
    (649, 'africa', 'Mali (Republic of)'),
    (650, 'africa', 'Mozambique (Republic of)'),
    (654, 'africa', 'Mauritania (Islamic Republic of)'),
    (655, 'africa', 'Malawi'),
    (656, 'africa', 'Niger (Republic of the)'),
    (657, 'africa', 'Nigeria (Federal Republic of)'),
    (659, 'africa', 'Namibia (Republic of)'),
    (660, 'africa', 'France - Reunion (French Department of)'),
    (661, 'africa', 'Rwanda (Republic of)'),
    (662, 'africa', 'Sudan (Republic of the)'),
    (663, 'africa', 'Senegal (Republic of)'),
    (664, 'africa', 'Seychelles (Republic of)'),
    (665, 'africa', 'United Kingdom of Great Britain and Northern Ireland - Saint Helena'),
    (666, 'africa', 'Somalia (Federal Republic of)'),
    (667, 'africa', 'Sierra Leone'),
    (668, 'africa', 'Sao Tome and Principe (Democratic Republic of)'),
    (669, 'africa', 'Eswatini (Kingdom of)'),
    (670, 'africa', 'Chad (Republic of)'),
    (671, 'africa', 'Togolese Republic'),
    (672, 'africa', 'Tunisia'),
    (674, 'africa', 'Tanzania (United Republic of)'),
    (675, 'africa', 'Uganda (Republic of)'),
    (676, 'africa', 'Democratic Republic of the Congo'),
    (677, 'africa', 'Tanzania (United Republic of)'),
    (678, 'africa', 'Zambia (Republic of)'),
    (679, 'africa', 'Zimbabwe (Republic of)'),
    (701, 'south america', 'Argentine Republic'),
    (710, 'south america', 'Brazil (Federative Republic of)'),
    (720, 'south america', 'Bolivia (Plurinational State of)'),
    (725, 'south america', 'Chile'),
    (730, 'south america', 'Colombia (Republic of)'),
    (735, 'south america', 'Ecuador'),
    (740, 'south america', 'United Kingdom of Great Britain and Northern Ireland - Falkland Islands (Malvinas)'),
    (745, 'south america', 'France - Guiana (French Department of)'),
    (750, 'south america', 'Guyana'),
    (755, 'south america', 'Paraguay (Republic of)'),
    (760, 'south america', 'Peru'),
    (765, 'south america', 'Suriname (Republic of)'),
    (770, 'south america', 'Uruguay (Eastern Republic of)'),
    (775, 'south america', 'Venezuela (Bolivarian Republic of)')
;

SELECT create_reference_table('mid_map');