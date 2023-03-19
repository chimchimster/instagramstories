import re

from pytesseract import pytesseract
from PIL import Image


class ImageHandling:
    def __init__(self, path_to_image: str) -> None:
        self.path_to_image = path_to_image

    def extract_text_from_image(self) -> str:
        # Open image with Pillow
        image = Image.open(self.path_to_image)

        # Extract text from image
        text = pytesseract.image_to_string(image, lang='eng+kaz+rus+ara+chi_tra+deu+fra+ita+jpn+kor+por+equ+fin')
        text = text.replace('\n', '')

        if text:
            # Escape from garbage symbols
            text = text.rstrip(' ')
            text = ''.join(re.sub(r'[^a-zA-ZА-Яа-я0-9\s]', '', text)).strip()

        return text if len(text) > 0 else 'empty'
