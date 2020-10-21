// 参考
// http://soraxism.com/soraxism/blog/html5%E3%81%AEfile-api%E3%82%92%E4%BD%BF%E7%94%A8%E3%81%97%E3%81%A6%E3%80%81%E3%83%89%E3%83%A9%E3%83%83%E3%82%B0%EF%BC%86%E3%83%89%E3%83%AD%E3%83%83%E3%83%97%E3%81%A7%E3%83%95%E3%82%A1%E3%82%A4
$(function(){
    /*================================================
     ファイルをドロップした時の処理
     =================================================*/
    $('#drag-area').bind('drop', function(e){
        // デフォルトの挙動を停止
        e.preventDefault();

        // ファイル情報を取得
        var files = e.originalEvent.dataTransfer.files;

        uploadFiles(files);

    }).bind('dragenter', function(){
        // デフォルトの挙動を停止
        return false;
    }).bind('dragover', function(){
        // デフォルトの挙動を停止
        return false;
    });

    /*================================================
     ダミーボタンを押した時の処理
     =================================================*/
    $('#btn').click(function() {
        // ダミーボタンとinput[type="file"]を連動
        $('input[type="file"]').click();
    });

    $('input[type="file"]').change(function(){
        // ファイル情報を取得
        var files = this.files;

        uploadFiles(files);
    });

});

/*================================================
 媒体チェック処理
 =================================================*/
function checkMedia() {
    var area = $('.dest-media:checked').map(function() {
        return $(this).val();
    }).get();
    return (area.length > 0);
}

/*================================================
 アップロード処理
 =================================================*/
function uploadFiles(files) {
    // FormDataオブジェクトを用意
    var fd = new FormData();

    // ファイルの個数を取得
    var filesLength = files.length;

    // filesは_FILESとして扱われるが、site, mediasは_POSTとして扱われる
    // http://blog.fagai.net/2013/04/23/fuelphp_upload/

    // ファイル情報を追加
    for (var i = 0; i < filesLength; i++) {
        fd.append("files[]", files[i]);
        // 複数ドロップでバグったら、ここでbreakすれば良いのでは？
    }

    // 選択した対象サイトを設定
    var val = $('[name=dest-site]').val();
    fd.append("target_site_id", val);

    $('#filter').fadeIn();

    var target = $('#form_upload-js-param').val();
    // Ajaxでアップロード処理をするファイルへ内容渡す
    $.ajax({
        url: location.pathname, // + 'api/link/'+target+'.json',
        type: 'POST',
        data: fd,
        processData: false,
        contentType: false,
        success: function(data) {
            $('#filter').fadeOut();
            if (data.redirect) {
                window.location.href = data.redirect;
            }
        }
    });
}
