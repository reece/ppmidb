truncate _code_list_overlay;
truncate _data_dictionary_overlay;

\copy _code_list_overlay from misc/code_list_overlay.csv with csv header;
\copy _data_dictionary_overlay from misc/data_dictionary_overlay.csv with csv header;
