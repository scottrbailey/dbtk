/* Apply HTML height/width attributes as inline styles so RTD theme can't override them */
document.addEventListener('DOMContentLoaded', function () {
    document.querySelectorAll('img[height]').forEach(function (img) {
        img.style.height = img.getAttribute('height') + 'px';
        img.style.width = 'auto';
        img.style.maxWidth = 'none';
    });
});
