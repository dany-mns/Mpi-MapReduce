#include <iostream>
#include <string>
#include "mpi.h"
#include <stdio.h>
#include <windows.h>
#include <math.h>
#include <vector>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <map>
#include <algorithm>
#include <ctime>

namespace fs = std::filesystem;

#define min(a, b) a>b? b:a
// De curatat intotdeaua fisierul de output (de map)


std::string generate_filename(const int len) {

	std::string tmp_s;
	int random_n = rand() % 100;
	static const char alphanum[] =
		"0123456789"
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
		"abcdefghijklmnopqrstuvwxyz";

	srand((unsigned)time(NULL) * GetCurrentProcessId());

	for (int i = 0; i < len; ++i)
		tmp_s += alphanum[rand() % (sizeof(alphanum) - 1)] + std::to_string(random_n);

	return tmp_s;

}

void write_file(std::map<std::string, int> idata, std::string filename) {
	std::ofstream myfile;
	myfile.open(filename);
	if (myfile.is_open()) {
		for (const auto& data : idata) {
			myfile << data.first << " " << data.second << std::endl;
		}
	}
	myfile.close();
}

void cleaning_text(std::string& text) {
	text.erase(std::remove(text.begin(), text.end(), '.'), text.end());
	text.erase(std::remove(text.begin(), text.end(), ','), text.end());
	text.erase(std::remove(text.begin(), text.end(), ';'), text.end());
	text.erase(std::remove(text.begin(), text.end(), ':'), text.end());
}


std::string read_file(const std::string& file_path) {
	const std::ifstream input_stream(file_path, std::ios_base::binary);
	if (input_stream.fail()) {
		throw std::runtime_error("Failed to open file");
	}

	std::stringstream buffer;
	buffer << input_stream.rdbuf();
	
	return buffer.str();
}

std::vector<std::string> split(std::string text) {
	std::vector<std::string> tokens;
	std::string buf;
	std::stringstream stream(text);

	while (stream >> buf)
		tokens.push_back(buf);
	return tokens;
}

std::vector<std::string> get_files(std::string directory) {
	std::vector<std::string> files;
	for (const auto& entry : fs::directory_iterator(directory))
		files.push_back(entry.path().string());
	return files;
}

std::map<std::string, int> get_frequency(std::vector<std::string> tokens) {
	std::map<std::string, int> frequency;

	for (const auto& word : tokens) {
		if (frequency.find(word) == frequency.end()) {
			frequency.insert(std::make_pair(word, 1));
		}
		else {
			frequency[word]++;
		}
	}
	return frequency;
}

std::map<std::string, int> read_map_from_file(std::string filename) {
	std::map<std::string, int> frequency;
	std::string line;
	std::ifstream myfile(filename);
	if (myfile.is_open()) {
		while (std::getline(myfile, line)) {
			std::vector<std::string> token = split(line);
			frequency.insert(std::make_pair(token[0], std::stoi(token[1])));
		}
	}
	return frequency;
}

std::map<std::string, int> get_freq_from_file(std::string path) {
	std::string content = read_file(path);
	cleaning_text(content);
	std::vector<std::string> tokens = split(content);
	std::map<std::string, int> frequency = get_frequency(tokens);
	return frequency;
}


int main(int argc, char** argv)
{
	int size, rank;
	MPI_Status status;
	MPI_Request request;

	std::string directory = "D:\\AC_2020-2021\\APD\\Proiect\\Directory";

	MPI_Init(NULL, NULL);
	MPI_Comm_size(MPI_COMM_WORLD, &size);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	/*
		Rank 0 => Master
		Rest ranks => Slave
	*/

	if (rank == 0) {
		printf("Start transmisie master=>slaves\n");
		std::vector<std::string> paths = get_files(directory);

		// am mai multe fisiere decat procese workeri (CAZ NASPA)
		if (paths.size() > size - 1) {
			int i = 0;
			bool procesat = true;
			int index_ph = 0;

			// se stie ca prima data toate procesele au de procesat
			for (int p = 1; p < size; p++) {
				MPI_Send(&procesat, 1, MPI_C_BOOL, p, 0, MPI_COMM_WORLD);
			}

			while (i < paths.size()) {
				int procese_necesare = min(size-1, (paths.size() - i));
				for (int p = 1; p <= procese_necesare; p++) {
					MPI_Send(paths[index_ph].c_str(), paths[index_ph].length() + 1, MPI_CHAR, p, 0, MPI_COMM_WORLD);
					//printf("%d\n", p);
					index_ph++;
					// vedem cine o terminat de procesat
					int slave = -1;
					MPI_Irecv(&slave, 1, MPI_INT, p, MPI_ANY_TAG, MPI_COMM_WORLD, &request);
				}
				i += procese_necesare;
				int cantitate = paths.size() - i;
				if (cantitate == 0) {
					// s-a terminat de procesat
					for (int p = 1; p <= procese_necesare; p++) {
						procesat = false;
						MPI_Send(&procesat, 1, MPI_C_BOOL, p, 0, MPI_COMM_WORLD);
					}
				}
				else if (cantitate >= size-1) {
					// mai este de procesat
					for (int p = 1; p < size; p++) {
						procesat = true;
						MPI_Send(&procesat, 1, MPI_C_BOOL, p, 0, MPI_COMM_WORLD);
					}
				}
				else {
					// mai trebuie sa se proceseze partial
					for (int p = 1; p <= cantitate; p++) {
						procesat = true;
						MPI_Send(&procesat, 1, MPI_C_BOOL, p, 0, MPI_COMM_WORLD);
					}
					for (int p = cantitate+1; p < size; p++) {
						procesat = false;
						MPI_Send(&procesat, 1, MPI_C_BOOL, p, 0, MPI_COMM_WORLD);
					}
				}
			}
		}
		else if (paths.size() < size - 1) {
			// mai mai multi workeri decat fisiere
			bool procesat = true;
			for (int p = 0; p < paths.size(); p++) {
				int p_idx = p + 1;
				MPI_Send(&procesat, 1, MPI_C_BOOL, p_idx, 0, MPI_COMM_WORLD);
				MPI_Send(paths[p].c_str(), paths[p].length() + 1, MPI_CHAR, p_idx, 0, MPI_COMM_WORLD);
			}
			procesat = false;
			for (int p = 1; p < size; p++) {
				// vedem cine o terminat de procesat
				int slave = -1;
				MPI_Irecv(&slave, 1, MPI_INT, p, MPI_ANY_TAG, MPI_COMM_WORLD, &request);
				MPI_Send(&procesat, 1, MPI_C_BOOL, p, 0, MPI_COMM_WORLD);
			}
		}
		else {
			// sunt egale nr de fisiere cu nr de procese
			bool procesat = true;
			for (int p = 0; p < paths.size(); p++) {
				procesat = true;
				int p_idx = p + 1;
				MPI_Send(&procesat, 1, MPI_C_BOOL, p_idx, 0, MPI_COMM_WORLD);
				MPI_Send(paths[p].c_str(), paths[p].length() + 1, MPI_CHAR, p_idx, 0, MPI_COMM_WORLD);
				procesat = false;

				// vedem cine o terminat de procesat
				int slave = -1;
				MPI_Irecv(&slave, 1, MPI_INT, p, MPI_ANY_TAG, MPI_COMM_WORLD, &request);

				MPI_Send(&procesat, 1, MPI_C_BOOL, p_idx, 0, MPI_COMM_WORLD);
			}
		}
	}
	else {
		bool procesat = true;
		MPI_Recv(&procesat, 1, MPI_C_BOOL, 0, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		while (procesat) {

			char path[256];
			MPI_Recv(path, 256, MPI_CHAR, 0, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

			std::map<std::string, int> frequency = get_freq_from_file(path);
			std::string output = "Map/" + generate_filename(8) + frequency.begin()->first + ".txt";
			write_file(frequency, output);
			MPI_Send(&rank, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
			MPI_Recv(&procesat, 1, MPI_C_BOOL, 0, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		}
	}

	MPI_Barrier(MPI_COMM_WORLD);

	if (rank == 0) {
		// trimite literele la procesat
		char letters[] = { 'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z', 
			'A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z' };
		int dim = sizeof(letters) / sizeof(char);

		// trimitem literele la procesat
		
		if (dim > size - 1) {
			int i = 0;
			bool procesat = true;
			int index_ph = 0;

			// se stie ca prima data toate procesele au de procesat
			for (int p = 1; p < size; p++) {
				MPI_Send(&procesat, 1, MPI_C_BOOL, p, 0, MPI_COMM_WORLD);
			}

			while (i < dim) {
				int procese_necesare = min(size - 1, (dim - i));
				for (int p = 1; p <= procese_necesare; p++) {
					MPI_Send(std::addressof(letters[index_ph]), 1, MPI_CHAR, p, 0, MPI_COMM_WORLD);

					index_ph++;
					// vedem cine o terminat de procesat
					int slave = -1;
					MPI_Irecv(&slave, 1, MPI_INT, p, MPI_ANY_TAG, MPI_COMM_WORLD, &request);
				}
				i += procese_necesare;
				int cantitate = dim - i;
				if (cantitate == 0) {
					// s-a terminat de procesat
					for (int p = 1; p <= procese_necesare; p++) {
						procesat = false;
						MPI_Send(&procesat, 1, MPI_C_BOOL, p, 0, MPI_COMM_WORLD);
					}
				}
				else if (cantitate >= size - 1) {
					// mai este de procesat
					for (int p = 1; p < size; p++) {
						procesat = true;
						MPI_Send(&procesat, 1, MPI_C_BOOL, p, 0, MPI_COMM_WORLD);
					}
				}
				else {
					// mai trebuie sa se proceseze partial
					for (int p = 1; p <= cantitate; p++) {
						procesat = true;
						MPI_Send(&procesat, 1, MPI_C_BOOL, p, 0, MPI_COMM_WORLD);
					}
					for (int p = cantitate + 1; p < size; p++) {
						procesat = false;
						MPI_Send(&procesat, 1, MPI_C_BOOL, p, 0, MPI_COMM_WORLD);
					}
				}
			}
		}
		else if (dim < size - 1) {
			// mai mai multi workeri decat fisiere
			bool procesat = true;
			for (int p = 0; p < dim; p++) {
				int p_idx = p + 1;
				MPI_Send(&procesat, 1, MPI_C_BOOL, p_idx, 0, MPI_COMM_WORLD);
				MPI_Send(std::addressof(letters[p]), 1, MPI_CHAR, p_idx, 0, MPI_COMM_WORLD);
			}
			procesat = false;
			for (int p = 1; p < size; p++) {
				// vedem cine o terminat de procesat
				int slave = -1;
				MPI_Irecv(&slave, 1, MPI_INT, p, MPI_ANY_TAG, MPI_COMM_WORLD, &request);
				MPI_Send(&procesat, 1, MPI_C_BOOL, p, 0, MPI_COMM_WORLD);
			}
		}
		else {
			// sunt egale nr de fisiere cu nr de procese
			bool procesat = true;
			for (int p = 0; p < dim; p++) {
				procesat = true;
				int p_idx = p + 1;
				MPI_Send(&procesat, 1, MPI_C_BOOL, p_idx, 0, MPI_COMM_WORLD);
				MPI_Send(std::addressof(letters[p]), 1, MPI_CHAR, p_idx, 0, MPI_COMM_WORLD);
				procesat = false;

				// vedem cine o terminat de procesat
				int slave = -1;
				MPI_Irecv(&slave, 1, MPI_INT, p, MPI_ANY_TAG, MPI_COMM_WORLD, &request);

				MPI_Send(&procesat, 1, MPI_C_BOOL, p_idx, 0, MPI_COMM_WORLD);
			}
		}


	}
	else {
		// scoate frecventele cuvintelor pe litera
		bool procesat = true;
		MPI_Recv(&procesat, 1, MPI_C_BOOL, 0, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		while (procesat) {

			char letter;
			MPI_Recv(&letter, 1, MPI_CHAR, 0, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			std::string str_letter(1, letter);
			
			std::map<std::string, int> set;
			std::vector<std::string> files = get_files("Map");
			for (int i = 0; i < files.size(); i++) {
				std::map<std::string, int> freq = get_freq_from_file(files[i]);
				for (const auto& f : freq) {
					if (f.first[0] == str_letter[0]) {
						if (set.find(f.first) == set.end()) {
							set[f.first] = f.second;
						}
						else {
							set[f.first] += f.second;
						}
					}
				}
			}
			std::string outfilename = "Reduce/" + str_letter + ".txt";
			
			if (!set.empty()) {
				write_file(set, outfilename);
				std::cout << "Complete for " << str_letter << std::endl;
			}

			MPI_Send(&rank, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
			MPI_Recv(&procesat, 1, MPI_C_BOOL, 0, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		}
	}
	
	MPI_Finalize();


	return 0;
}
