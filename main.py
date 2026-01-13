import sys
import os
import pandas as pd
import shutil
import tempfile
import traceback
from PySide6.QtWidgets import (QApplication, QMainWindow, QVBoxLayout, QHBoxLayout, 
                               QWidget, QLabel, QLineEdit, QPushButton, QProgressBar, 
                               QMessageBox, QFileDialog, QGroupBox, QSpinBox, QTextEdit, QRadioButton)
from PySide6.QtCore import Qt, QThread, Signal
import openpyxl

class SearchWorker(QThread):
    """
    Рабочий поток для выполнения поиска в Excel-файлах.
    Выполняет ресурсоёмкие операции в фоновом режиме, чтобы не блокировать UI.
    
    Сигналы:
        progress (int): процент выполнения задачи (0–100).
        message (str): текстовое сообщение о текущем этапе.
        finished (bool, str): результат выполнения (успех/ошибка) и итоговое сообщение.
    """
    progress = Signal(int)
    message = Signal(str)
    finished = Signal(bool, str)
    
    def __init__(self, search_values, directory, column_index, selected_columns, output_file, sheets_mode):
        """
        Инициализация рабочего потока.

        Параметры:
            search_values (list): список значений для поиска.
            directory (str): путь к директории с Excel-файлами.
            column_index (int): номер столбца для поиска (1-индексный).
            selected_columns (list): номера столбцов для копирования результатов.
            output_file (str): путь к файлу для сохранения результатов.
            sheets_mode (str or list): режим выбора листов ('first', 'all' или список названий).
        """
        super().__init__()
        self.search_values = search_values
        self.directory = directory
        self.column_index = column_index
        self.selected_columns = selected_columns
        self.output_file = output_file
        self.sheets_mode = sheets_mode
        self.is_running = True
        
    def read_excel_safely(self, file_path):
        """
        Безопасное чтение Excel файла через временную копию с валидацией
        """
        temp_path = None
        try:
            filename = os.path.basename(file_path)
            
            if filename.startswith('~$'):
                raise ValueError(f"Файл {filename} является временным файлом Excel")
            
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Файл {filename} не существует")
            
            file_size = os.path.getsize(file_path)
            max_size = 500 * 1024 * 1024
            if file_size > max_size:
                raise ValueError(f"Файл {filename} слишком большой: {file_size // (1024*1024)}MB")
            
            temp_dir = tempfile.gettempdir()
            temp_filename = f"excel_search_temp_{os.urandom(8).hex()}.xlsx"
            temp_path = os.path.join(temp_dir, temp_filename)
            
            shutil.copy2(file_path, temp_path)
            
            if not os.path.exists(temp_path):
                raise IOError(f"Не удалось создать временную копию файла {filename}")

            xls = pd.ExcelFile(temp_path, engine='openpyxl')
            
            return xls, temp_path
            
        except PermissionError:
            if temp_path and os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except:
                    pass
            raise PermissionError(f"Файл {filename} занят другим процессом или недостаточно прав")
        except Exception as e:
            if temp_path and os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except:
                    pass
            raise Exception(f"Ошибка чтения файла {filename}: {str(e)}")

    def safe_delete_temp_file(self, temp_path):
        """
        Безопасное удаление временного файла.
        """
        if temp_path and os.path.exists(temp_path):
            try:
                temp_dir = tempfile.gettempdir()
                if os.path.commonprefix([os.path.abspath(temp_path), os.path.abspath(temp_dir)]) == os.path.abspath(temp_dir):
                    os.unlink(temp_path)
            except Exception as e:
                self.message.emit(f"Не удалось удалить временный файл: {e}")

    def is_safe_to_delete(self, file_path):
        """
        Проверяет, что файл можно безопасно удалить
        """
        try:
            if not file_path:
                return False

            if not os.path.exists(file_path):
                return False
                
            temp_dir = tempfile.gettempdir()
            file_path_abs = os.path.abspath(file_path)
            temp_dir_abs = os.path.abspath(temp_dir)
            
            if not os.path.commonpath([file_path_abs, temp_dir_abs]) == temp_dir_abs:
                return False

            filename = os.path.basename(file_path)
            if not filename.startswith('excel_search_temp_'):
                return False

            if not os.path.isfile(file_path):
                return False

            if not filename.lower().endswith(('.xlsx', '.xls', '.xlsm', '.xlsb')):
                return False
                
            return True
            
        except Exception:
            return False

    def get_excel_files_safely(self, directory):
        """
        Получение списка Excel файлов с пропуском временных файлов и валидацией
        """
        excel_files = []
        
        if not os.path.exists(directory):
            self.message.emit(f"Директория не существует: {directory}")
            return []
        
        if not os.path.isdir(directory):
            self.message.emit(f"Указанный путь не является директорией: {directory}")
            return []
        
        try:
            for file in os.listdir(directory):
                file_path = os.path.join(directory, file)
                
                if file.startswith('~$'):
                    self.message.emit(f"Пропущен временный файл Excel: {file}")
                    continue
                
                if not os.path.isfile(file_path):
                    continue
                
                file_lower = file.lower()
                if file_lower.endswith(('.xlsx', '.xls', '.xlsm', '.xlsb')):
                    excel_files.append(file_path)
        
        except PermissionError:
            self.message.emit(f"Нет доступа к директории: {directory}")
        except Exception as e:
            self.message.emit(f"Ошибка при чтении директории: {str(e)}")
        
        return excel_files
    
    def run(self):
        """
        Основной метод выполнения поиска. Вызывается автоматически при старте потока.
        """
        temp_files_to_cleanup = [] 
        
        try:
            try:
                self.validate_output_path(self.output_file, self.directory)
            except ValueError as e:
                self.finished.emit(False, f"Ошибка пути: {str(e)}")
                return
            except Exception as e:
                self.finished.emit(False, f"Ошибка проверки пути: {str(e)}")
                return
            
            self.message.emit("Поиск Excel файлов...")
            
            excel_files = self.get_excel_files_safely(self.directory)
            
            if not excel_files:
                self.finished.emit(False, "В указанной директории не найдено Excel файлов")
                return
            
            self.message.emit(f"Найдено {len(excel_files)} Excel файлов")
            
            search_data = []  
            for value in self.search_values:
                str_value = str(value).strip()
                
                if not str_value:
                    continue
                
                search_variants = [
                    str_value,
                    str_value.lower(),
                    str_value.upper(),
                    str_value.replace(' ', '')
                ]
                
                try:
                    num_value = float(value)
                    search_variants.append(str(int(num_value)))
                    search_variants.append(str(num_value))
                    if num_value == int(num_value):
                        search_variants.append(str(int(num_value)))
                except (ValueError, TypeError):
                    pass
                
                search_data.append((str_value, search_variants))
            
            if not search_data:
                self.finished.emit(False, "Нет валидных значений для поиска")
                return
            
            all_results = []
            error_results = []
            locked_files = []  
            total_files = len(excel_files)
            found_count = 0
            
            for i, file_path in enumerate(excel_files):
                if not self.is_running:
                    self.message.emit("Поиск прерван пользователем")
                    break
                    
                self.message.emit(f"Обработка файла: {os.path.basename(file_path)}")
                self.progress.emit(int((i / total_files) * 100))
                
                xls = None
                temp_file_path = None
                
                try:
                    xls, temp_file_path = self.read_excel_safely(file_path)
                    if temp_file_path:
                        temp_files_to_cleanup.append(temp_file_path)
                    
                    sheet_names = xls.sheet_names

                    sheets_to_process = []
                    if self.sheets_mode == "first":
                        sheets_to_process = [sheet_names[0]] if sheet_names else []
                    elif self.sheets_mode == "all":
                        sheets_to_process = sheet_names
                    elif isinstance(self.sheets_mode, list):
                        sheets_to_process = [sheet for sheet in self.sheets_mode if sheet in sheet_names]
                    
                    if not sheets_to_process:
                        error_msg = f"В файле {os.path.basename(file_path)} нет указанных листов"
                        self.message.emit(error_msg)
                        
                        error_row = [error_msg]
                        for _ in self.selected_columns:
                            error_row.append("")
                        error_row.append(os.path.basename(file_path))
                        error_results.append(error_row)
                        continue

                    for sheet_name in sheets_to_process:
                        try:
                            df = pd.read_excel(xls, sheet_name=sheet_name, header=None, engine='openpyxl')
                            
                            if df is None or df.empty:
                                continue
                            
                            if self.column_index - 1 >= len(df.columns):
                                error_msg = f"В файле {os.path.basename(file_path)} (лист '{sheet_name}') нет столбца {self.column_index}"
                                self.message.emit(error_msg)
                                
                                error_row = [error_msg]
                                for _ in self.selected_columns:
                                    error_row.append("")
                                error_row.append(f"{os.path.basename(file_path)} (лист: {sheet_name})")
                                error_results.append(error_row)
                                continue
                            
                            search_column = df.iloc[:, self.column_index - 1]
                            
                            for idx in range(len(df)):
                                if not self.is_running:
                                    break
                                    
                                cell_value = search_column.iloc[idx]
                                if pd.isna(cell_value):
                                    continue
                                
                                cell_str = str(cell_value)
                                found_in_cell = []
                                
                                for search_original, search_variants in search_data:
                                    for search_variant in search_variants:
                                        if self.is_exact_match(cell_str, search_variant):
                                            found_in_cell.append(search_original)
                                            break  
                                
                                for found_value in found_in_cell:
                                    result_row = []
                                    result_row.append(found_value)
                                    
                                    for col_idx in self.selected_columns:
                                        if col_idx - 1 < len(df.columns):
                                            value = df.iloc[idx, col_idx - 1]
                                            result_row.append(value if pd.notna(value) else "")
                                        else:
                                            result_row.append("")
                                    
                                    result_row.append(f"{os.path.basename(file_path)} (лист: {sheet_name})")
                                    all_results.append(result_row)
                                    found_count += 1
                                    
                        except Exception as e:
                            error_msg = f"Ошибка при обработке листа '{sheet_name}' в файле {os.path.basename(file_path)}: {str(e)}"
                            self.message.emit(error_msg)
                            
                            error_row = [error_msg]
                            for _ in self.selected_columns:
                                error_row.append("")
                            error_row.append(f"{os.path.basename(file_path)} (лист: {sheet_name})")
                            error_results.append(error_row)
                            continue
                            
                except PermissionError as e:
                    locked_files.append(os.path.basename(file_path))
                    error_msg = f"Файл {os.path.basename(file_path)} занят другим процессом"
                    self.message.emit(error_msg)
                    
                    error_row = [error_msg]
                    for _ in self.selected_columns:
                        error_row.append("")
                    error_row.append(os.path.basename(file_path))
                    error_results.append(error_row)
                    
                except Exception as e:
                    error_msg = f"Ошибка при обработке файла {file_path}: {str(e)}"
                    self.message.emit(error_msg)
                    
                    error_row = [error_msg]
                    for _ in self.selected_columns:
                        error_row.append("")
                    error_row.append(os.path.basename(file_path))
                    error_results.append(error_row)
                finally:
                    try:
                        if xls is not None:
                            xls.close()
                    except:
                        pass
            
            combined_results = all_results + error_results
            
            if combined_results:
                headers = ["Искомые значения"]
                headers.extend([f"Столбец {i}" for i in self.selected_columns])
                headers.append("Файл источника")
                
                result_df = pd.DataFrame(combined_results, columns=headers)
                
                try:
                    output_dir = os.path.dirname(self.output_file)
                    if output_dir and not os.path.exists(output_dir):
                        os.makedirs(output_dir, exist_ok=True)
                    
                    if os.path.abspath(self.output_file) in [os.path.abspath(f) for f in excel_files]:
                        raise ValueError("Файл результатов не может совпадать с исходными файлами поиска")
                    
                    with pd.ExcelWriter(self.output_file, engine='openpyxl') as writer:
                        result_df.to_excel(writer, sheet_name='Результаты', index=False)
                        
                        workbook = writer.book
                        worksheet = writer.sheets['Результаты']
                        
                        for col in worksheet.columns:
                            column_letter = openpyxl.utils.get_column_letter(col[0].column)
                            worksheet.column_dimensions[column_letter].width = 30
                    
                    success_count = len(all_results)
                    error_count = len(error_results)
                    
                    if locked_files:
                        locked_info = f"\n\nЗанятые файлы ({len(locked_files)}):\n" + "\n".join(locked_files)
                    else:
                        locked_info = ""
                    
                    final_message = f"Поиск завершен.\nНайдено: {success_count} совпадений\nОшибок: {error_count}{locked_info}"
                    
                    self.message.emit(f"Найдено {success_count} совпадений, ошибок: {error_count}, занятых файлов: {len(locked_files)}")
                    self.finished.emit(True, final_message)
                    
                except PermissionError:
                    self.finished.emit(False, f"Ошибка: Файл результатов {self.output_file} занят. Закройте его и повторите попытку.")
                except Exception as e:
                    self.finished.emit(False, f"Ошибка при сохранении результатов: {str(e)}")
            else:
                if locked_files:
                    locked_info = f"\n\nЗанятые файлы ({len(locked_files)}):\n" + "\n".join(locked_files)
                    final_message = f"Совпадений не найдено.{locked_info}"
                else:
                    final_message = "Совпадений не найдено"
                self.finished.emit(True, final_message)
                
        except Exception as e:
            error_trace = traceback.format_exc()
            print(f"Критическая ошибка: {error_trace}")
            self.finished.emit(False, f"Критическая ошибка: {str(e)}")
        finally:
            for temp_file in temp_files_to_cleanup:
                self.safe_delete_temp_file(temp_file)
            
            try:
                if 'xls' in locals() and xls is not None:
                    xls.close()
            except:
                pass

    def is_exact_match(self, cell_str, search_variant):
        """
        Проверяет точное совпадение search_variant в cell_str с учетом разделителей
        """
        if not cell_str or not search_variant:
            return False

        normalized_cell = cell_str
        normalized_cell = normalized_cell.replace('\r\n', ' ').replace('\r', ' ').replace('\n', ' ')
        normalized_cell = normalized_cell.replace(',', ' ').replace(';', ' ')
        normalized_cell = normalized_cell.replace('\t', ' ')
        parts = normalized_cell.split()
        for part in parts:
            part_clean = part.strip('.,;:!?()[]{}"\'')
            if not part_clean:
                continue
            if part_clean == search_variant:
                return True
            if part_clean.replace(' ', '') == search_variant:
                return True
        
        return False
    
    def validate_output_path(self, output_file, search_directory):
        """
        Проверяет что выходной файл не находится в директории поиска
        и не совпадает с исходными файлами.
        """
        output_abs = os.path.abspath(output_file)
        search_abs = os.path.abspath(search_directory)
        
        if os.path.dirname(output_abs) == search_abs:
            raise ValueError("Файл результатов нельзя сохранять в той же папке, где находятся исходные файлы")
        
        if os.path.exists(output_abs):
            excel_files = self.get_excel_files_safely(search_directory)
            for file_path in excel_files:
                if os.path.abspath(file_path) == output_abs:
                    raise ValueError(f"Файл результатов совпадает с исходным файлом: {os.path.basename(file_path)}")
        
        return True
    
    def stop(self):
        """
        Остановка поиска.
        """
        self.is_running = False


class ExcelSearchApp(QMainWindow):
    """
    Главное окно приложения для поиска в Excel файлах.
    Предоставляет GUI для настройки параметров поиска, запуска процесса
    и отображения прогресса/результатов.
    """
    def __init__(self):
        """
        Создает UI элементы и подключает обработчики событий.
        """
        super().__init__()
        self.search_worker = None
        self.init_ui()
        
    def init_ui(self):
        """
        Инициализация пользовательского интерфейса.
        Создает все виджеты, layouts и подключает сигналы.
        """
        self.setWindowTitle("Программа для поиска информации в Excel файлах")
        self.setGeometry(100, 100, 500, 940)
        
        # Центральный виджет
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # Основной layout
        main_layout = QVBoxLayout(central_widget)
        
        # Группа для ввода значений
        values_group = QGroupBox("")
        values_layout = QVBoxLayout(values_group)
        
        # Единое поле для ввода значений
        self.values_input = QTextEdit()
        self.values_input.setPlaceholderText(
            "Введите значения для поиска (каждое значение с новой строки)."
        )
        self.values_input.setMinimumHeight(300)
        values_layout.addWidget(self.values_input)
        
        main_layout.addWidget(values_group)
        
        # Группа для директории с Excel файлами
        dir_group = QGroupBox("Папка с Excel файлами")
        dir_layout = QHBoxLayout(dir_group)
        self.dir_input = QLineEdit()
        self.dir_input.setReadOnly(True)
        dir_layout.addWidget(self.dir_input)
        self.browse_dir_button = QPushButton("Выбрать...")
        self.browse_dir_button.clicked.connect(self.browse_directory)
        dir_layout.addWidget(self.browse_dir_button)
        main_layout.addWidget(dir_group)

        # Группа для сохранения результатов
        save_group = QGroupBox("Папка сохранения результатов")
        save_layout = QHBoxLayout(save_group)
        self.save_input = QLineEdit()
        self.save_input.setReadOnly(True)
        save_layout.addWidget(self.save_input)
        self.browse_save_button = QPushButton("Выбрать...")
        self.browse_save_button.clicked.connect(self.browse_save_location)
        save_layout.addWidget(self.browse_save_button)
        main_layout.addWidget(save_group)
        
        # Группа для столбца поиска
        search_column_group = QGroupBox("Столбец для поиска")
        search_column_layout = QHBoxLayout(search_column_group)
        self.column_spinbox = QSpinBox()
        self.column_spinbox.setMinimum(1)
        self.column_spinbox.setMaximum(999999)
        search_column_layout.addWidget(self.column_spinbox)
        main_layout.addWidget(search_column_group)
        
        # Группа для номеров столбцов
        columns_group = QGroupBox("Номера столбцов (через запятую)")
        columns_layout = QVBoxLayout(columns_group)
        self.columns_input = QLineEdit()
        self.columns_input.setPlaceholderText("Введите номера столбцов через запятую. Например: 1,3,5")
        columns_layout.addWidget(self.columns_input)
        main_layout.addWidget(columns_group)
        
        # Группа для выбора листов
        sheets_group = QGroupBox("Выбор листов для поиска")
        sheets_layout = QVBoxLayout(sheets_group)
        
        # Радиокнопки для выбора режима
        self.sheets_first_rb = QRadioButton("Текущий (первый лист) - по умолчанию")
        self.sheets_first_rb.setChecked(True)
        self.sheets_all_rb = QRadioButton("Все листы")
        self.sheets_custom_rb = QRadioButton("Указать листы:")
        
        sheets_layout.addWidget(self.sheets_first_rb)
        sheets_layout.addWidget(self.sheets_all_rb)
        sheets_layout.addWidget(self.sheets_custom_rb)
        
        # Поле для ввода листов
        self.sheets_input = QLineEdit()
        self.sheets_input.setPlaceholderText("Введите названия листов через запятую. Например: Таблица,Данные")
        self.sheets_input.setEnabled(False)
        sheets_layout.addWidget(self.sheets_input)
        
        # Подключаем переключатели
        self.sheets_first_rb.toggled.connect(self.on_sheets_mode_changed)
        self.sheets_all_rb.toggled.connect(self.on_sheets_mode_changed)
        self.sheets_custom_rb.toggled.connect(self.on_sheets_mode_changed)
        
        main_layout.addWidget(sheets_group)
        
        # Прогресс-бар
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        main_layout.addWidget(self.progress_bar)
        
        # Статус
        self.status_label = QLabel("Готов к работе")
        self.status_label.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(self.status_label)
        
        # Кнопки управления
        buttons_layout = QHBoxLayout()
        buttons_layout.addStretch()
        self.search_button = QPushButton("Начать поиск")
        self.search_button.setMinimumHeight(10)
        self.search_button.setMinimumWidth(160)
        self.search_button.setStyleSheet("margin-bottom: 10px; margin-top: 20px; padding: 4px")
        self.search_button.clicked.connect(self.start_search)
        buttons_layout.addWidget(self.search_button)
        buttons_layout.addStretch()
        main_layout.addLayout(buttons_layout)
    
    def on_sheets_mode_changed(self):
        """Включение/отключение поля ввода листов"""
        self.sheets_input.setEnabled(self.sheets_custom_rb.isChecked())
    
    def get_selected_sheets(self):
        """Получение выбранных листов"""
        if self.sheets_first_rb.isChecked():
            return "first"  
        elif self.sheets_all_rb.isChecked():
            return "all"    
        elif self.sheets_custom_rb.isChecked():
            text = self.sheets_input.text().strip()
            if text:
                sheets = [sheet.strip() for sheet in text.split(',') if sheet.strip()]
                return sheets
        return "first" 
    
    def browse_directory(self):
        """
        Открытие диалога выбора директории с Excel файлами.
        """
        directory = QFileDialog.getExistingDirectory(self, "Выберите директорию с Excel файлами")
        if directory:
            self.dir_input.setText(directory)
    
    def browse_save_location(self):
        """
        Устанавливаем имя файла по умолчанию
        """
        default_file = "Результат_поиска.xlsx"
        
        file_path, _ = QFileDialog.getSaveFileName(
            self, 
            "Сохранить результаты поиска", 
            default_file,
            "Excel Files (*.xlsx)"
        )
        if file_path:
            if not file_path.endswith('.xlsx'):
                file_path += '.xlsx'
            self.save_input.setText(file_path)
    
    def get_selected_columns(self):
        """
        Получение выбранных столбцов из строки ввода
        """
        input_text = self.columns_input.text().strip()
        if not input_text:
            return []
        
        try:
            columns = []
            for part in input_text.split(','):
                part = part.strip()
                if part:
                    column_num = int(part)
                    if column_num > 0:
                        columns.append(column_num)
            return sorted(set(columns)) 
        except ValueError:
            return []
    
    def get_search_values(self):
        """
        Получение значений для поиска из текстового поля
        """
        text = self.values_input.toPlainText().strip()
        if not text:
            return []
        
        values = [v.strip() for v in text.split('\n') if v.strip()]
        return values
    
    def validate_input(self):
        """
        Проверка корректности введенных данных
        """
        search_values = self.get_search_values()
        if not search_values:
            QMessageBox.warning(self, "Ошибка", "Введите хотя бы одно значение для поиска")
            return False
        
        if not self.dir_input.text():
            QMessageBox.warning(self, "Ошибка", "Выберите директорию с Excel файлами")
            return False
        
        if not self.save_input.text():
            QMessageBox.warning(self, "Ошибка", "Выберите место сохранения результатов")
            return False
        
        column_index = self.column_spinbox.value()
        if column_index < 1:
            QMessageBox.warning(self, "Ошибка", "Номер столбца должен быть положительным числом")
            return False
        
        selected_columns = self.get_selected_columns()
        if not selected_columns:
            QMessageBox.warning(self, "Ошибка", "Введите хотя бы один номер столбца для копирования")
            return False
        
        return True
    
    def start_search(self):
        """
        Запуск процесса поиска.
        Валидирует ввод, создает и запускает SearchWorker.
        """
        if not self.validate_input():
            return
        
        if self.search_worker and self.search_worker.isRunning():
            QMessageBox.information(self, "Внимание", "Поиск уже выполняется")
            return
        
        search_values = self.get_search_values()
        directory = self.dir_input.text()
        column_index = self.column_spinbox.value()
        selected_columns = self.get_selected_columns()
        output_file = self.save_input.text()
        sheets_mode = self.get_selected_sheets()
        
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        self.search_button.setEnabled(False)
        
        self.search_worker = SearchWorker(
            search_values, directory, column_index, selected_columns, output_file, sheets_mode
        )
        self.search_worker.progress.connect(self.progress_bar.setValue)
        self.search_worker.message.connect(self.status_label.setText)
        self.search_worker.finished.connect(self.search_finished)
        self.search_worker.start()
    
    def search_finished(self, success, message):
        """
        Обработчик завершения поиска.
        """
        self.progress_bar.setVisible(False)
        self.search_button.setEnabled(True)
        self.status_label.setText(message)
        
        if success:
            QMessageBox.information(self, "Успех", message)
        else:
            QMessageBox.critical(self, "Ошибка", message)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    
    app.setStyle('Fusion')
    window = ExcelSearchApp()
    window.show()
    
    sys.exit(app.exec())
