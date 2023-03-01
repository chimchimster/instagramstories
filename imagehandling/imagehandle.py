import os
import re

from pytesseract import pytesseract
from PIL import Image


class ImageHandling:
    def __init__(self, path_to_folder: str) -> None:
        self.path_to_folder = path_to_folder

    def collect_paths_of_images(self):
        return [self.path_to_folder + '/' + jpg for jpg in os.listdir(self.path_to_folder) if jpg.endswith('.jpg')]

    def extract_text_from_images(self):
        text_image = {}
        paths = self.collect_paths_of_images()

        for path in paths:
            # Open image with Pillow
            image = Image.open(path)

            # Extract text from image
            text = pytesseract.image_to_string(image, lang='eng+rus')
            text = text.replace('\n', '')

            if text:
                # Update text_image dictionary
                text_image[path] = text

        return text_image

    def create_txt_files(self):
        data = self.extract_text_from_images()
        for key, value in data.items():
            with open(key.rstrip('jpg') + 'txt', 'w') as file:
                x = value.replace(' ', '')
                print(x, len(x))
                if len(x) > 0:
                    file.write(' '.join(re.sub(r'[^a-zA-ZА-Яа-я0-9\s]', '', value).split()))
                else:
                    continue